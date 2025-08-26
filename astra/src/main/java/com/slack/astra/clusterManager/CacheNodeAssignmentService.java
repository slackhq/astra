package com.slack.astra.clusterManager;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.astra.server.AstraConfig.DEFAULT_ZK_TIMEOUT_SECS;
import static com.slack.astra.util.FutureUtils.successCountingCallback;
import static com.slack.astra.util.TimeUtils.nanosToMillis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.replica.ReplicaMetadata;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.crt.Log;

/**
 * Service responsible for managing the assignment & cleanup of replicas to cache nodes.
 *
 * <p>Periodically assign replicas to cache nodes based on the configured replica sets. It fetches
 * metadata from various stores, calculates the assignments, and persists them in ZK. Metrics are
 * tracked for assignment and eviction operations.
 */
public class CacheNodeAssignmentService extends AbstractScheduledService {
  private ScheduledFuture<?> pendingTask;
  private final AstraConfigs.ManagerConfig managerConfig;
  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setNameFormat("cache-node-assignment-service-%d").build());
  private final MeterRegistry meterRegistry;
  private final ReplicaMetadataStore replicaMetadataStore;
  private final CacheNodeMetadataStore cacheNodeMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final CacheNodeAssignmentStore cacheNodeAssignmentStore;

  private final AstraMetadataStoreChangeListener<CacheNodeMetadata> cacheNodeListener =
      (cacheNodeMetadata) -> runOneIteration();
  private final AstraMetadataStoreChangeListener<ReplicaMetadata> replicaListener =
      (replicaMetadata) -> runOneIteration();
  private final AstraMetadataStoreChangeListener<CacheNodeAssignment> cacheNodeAssignmentListener =
      this::assignmentListener;

  protected static final Logger LOG = LoggerFactory.getLogger(CacheNodeAssignmentService.class);
  private static final String NEW_BIN_PREFIX = "NEW_";
  @VisibleForTesting protected static int futuresListTimeoutSecs = DEFAULT_ZK_TIMEOUT_SECS;

  public static final String ASSIGNMENT_CREATE_SUCCEEDED = "assignment_create_succeeded";
  public static final String ASSIGNMENT_CREATE_FAILED = "assignment_create_failed";
  public static final String ASSIGNMENT_CREATE_TIMER = "assignment_create_timer";
  public static final String EVICT_ASSIGNMENT_TIMER = "evict_assignment_timer";
  public static final String EVICT_SUCCEEDED = "evict_succeeded";
  public static final String EVICT_FAILED = "evict_failed";

  protected final Counter assignmentCreateSucceeded;
  protected final Counter assignmentCreateFailed;
  protected final Counter evictSucceeded;
  protected final Counter evictFailed;
  private final Timer assignmentCreateTimer;
  private final Timer evictAssignmentTimer;

  public CacheNodeAssignmentService(
      MeterRegistry meterRegistry,
      AstraConfigs.ManagerConfig managerConfig,
      ReplicaMetadataStore replicaMetadataStore,
      CacheNodeMetadataStore cacheNodeMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      CacheNodeAssignmentStore cacheNodeAssignmentStore) {
    this.managerConfig = managerConfig;
    this.meterRegistry = meterRegistry;
    this.replicaMetadataStore = replicaMetadataStore;
    this.cacheNodeMetadataStore = cacheNodeMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.cacheNodeAssignmentStore = cacheNodeAssignmentStore;

    assignmentCreateSucceeded = meterRegistry.counter(ASSIGNMENT_CREATE_SUCCEEDED);
    assignmentCreateFailed = meterRegistry.counter(ASSIGNMENT_CREATE_FAILED);
    assignmentCreateTimer = meterRegistry.timer(ASSIGNMENT_CREATE_TIMER);
    evictAssignmentTimer = meterRegistry.timer(EVICT_ASSIGNMENT_TIMER);
    evictSucceeded = meterRegistry.counter(EVICT_SUCCEEDED);
    evictFailed = meterRegistry.counter(EVICT_FAILED);
  }

  @Override
  protected synchronized void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executorService.schedule(
              this::assignReplicasToCacheNodes,
              managerConfig.getEventAggregationSecs(),
              TimeUnit.SECONDS);
    } else {
      LOG.debug(
          "Cache node assignment task already scheduled, will run in {} ms",
          pendingTask.getDelay(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting cache node assignment service");
    cacheNodeMetadataStore.addListener(cacheNodeListener);
    replicaMetadataStore.addListener(replicaListener);
    cacheNodeAssignmentStore.addListener(cacheNodeAssignmentListener);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing cache node assignment service");
    replicaMetadataStore.removeListener(replicaListener);
    cacheNodeMetadataStore.removeListener(cacheNodeListener);
    executorService.shutdownNow();
    meterRegistry.close();
    LOG.info("Closed cache node assignment service");
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        managerConfig.getScheduleInitialDelayMins(),
        managerConfig.getCacheNodeAssignmentServiceConfig().getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  /**
   * Assigns replicas to cache nodes based on the configured replica sets. This method fetches the
   * list of replicas and cache nodes. Then, for each replica set, marks assignments for eviction if
   * necessary, and calculates new assignments.
   */
  protected void assignReplicasToCacheNodes() {
    Timer.Sample assignmentTimer = Timer.start(meterRegistry);
    List<String> replicaSets =
        managerConfig.getCacheNodeAssignmentServiceConfig().getReplicaSetsList();
    Instant now = Instant.now();

    for (String replicaSet : replicaSets) {
      List<ReplicaMetadata> replicas =
          replicaMetadataStore.listSync().stream()
              .filter(replicaMetadata -> replicaSet.equals(replicaMetadata.getReplicaSet()))
              .toList();
      List<CacheNodeMetadata> cacheNodes =
          cacheNodeMetadataStore.listSync().stream()
              .filter(cacheNodeMetadata -> replicaSet.equals(cacheNodeMetadata.getReplicaSet()))
              .toList();
      List<CacheNodeAssignment> currentAssignments =
          cacheNodeAssignmentStore.listSync().stream()
              .filter(assignment -> replicaSet.equals(assignment.replicaSet))
              .toList();

      markAssignmentsForEviction(currentAssignments, replicaMetadataBySnapshotId(replicas), now);

      Map<String, SnapshotMetadata> snapshotIdsToMetadata =
          snapshotMetadataBySnapshotId(snapshotMetadataStore);

      List<SnapshotMetadata> assignedSnapshots =
          getSnapshotsFromIds(
              snapshotIdsToMetadata,
              currentAssignments.stream().map(x -> x.snapshotId).collect(Collectors.toSet()));
      List<SnapshotMetadata> snapshotsWithReplicas =
          getSnapshotsFromIds(
              snapshotIdsToMetadata,
              replicas.stream()
                  .filter(replica -> replica.expireAfterEpochMs > now.toEpochMilli())
                  .map(x -> x.snapshotId)
                  .collect(Collectors.toSet()));
      // Unassigned snapshots are the difference between snapshots with replicas and assigned
      // snapshots
      List<SnapshotMetadata> unassignedSnapshots =
          getUnassignedSnapshots(snapshotsWithReplicas, assignedSnapshots);
      unassignedSnapshots.sort(Comparator.comparing(a -> a.snapshotId));

      Map<String, CacheNodeBin> newAssignments =
          assign(
              cacheNodeAssignmentStore,
              snapshotMetadataStore,
              currentAssignments,
              unassignedSnapshots,
              cacheNodes);
      int successfulAssignments =
          persistAssignments(
              cacheNodeAssignmentStore,
              newAssignments,
              replicaSet,
              replicaMetadataBySnapshotId(replicas),
              cacheNodesByLoadingAssignments(currentAssignments),
              managerConfig.getCacheNodeAssignmentServiceConfig().getMaxConcurrentPerNode());
      int skippedAssignments = replicas.size() - successfulAssignments;

      assignmentCreateSucceeded.increment(successfulAssignments);

      long evictionDuration = assignmentTimer.stop(assignmentCreateTimer);
      LOG.info(
          "Completed cache node assignments for {} - successfully assigned {} replicas ({} total snapshots), skipped {} out of {} replicas in {} ms",
          replicaSet,
          successfulAssignments,
          unassignedSnapshots.size(),
          skippedAssignments,
          replicas.size(),
          nanosToMillis(evictionDuration));
    }
  }

  private Map<String, Integer> cacheNodesByLoadingAssignments(
      List<CacheNodeAssignment> currentAssignments) {
    Map<String, Integer> cacheNodesByAssignments = new HashMap<>();

    for (CacheNodeAssignment assignment : currentAssignments) {
      if (cacheNodesByAssignments.containsKey(assignment.cacheNodeId)
          && assignment.state == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING) {
        cacheNodesByAssignments.compute(
            assignment.cacheNodeId, (k, currentValue) -> currentValue + 1);
      } else {
        if (assignment.state == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING) {
          cacheNodesByAssignments.put(assignment.cacheNodeId, 1);
        }
      }
    }
    return cacheNodesByAssignments;
  }

  /**
   * Marks assignments for eviction if they meet certain criteria (currently live, and has an
   * expiration in the past). This method filters assignments that should be evicted based on the
   * provided expiration time and metadata, updates their state, and logs the results.
   *
   * @param cacheNodeAssignments the list of cache node assignments to evaluate
   * @param replicaMetadataBySnapshotId a map of replica metadata keyed by snapshot ID
   * @param expireOlderThan the cutoff time for marking assignments for eviction
   * @return the number of successful evictions
   */
  @VisibleForTesting
  public int markAssignmentsForEviction(
      List<CacheNodeAssignment> cacheNodeAssignments,
      Map<String, ReplicaMetadata> replicaMetadataBySnapshotId,
      Instant expireOlderThan) {
    Timer.Sample evictionTimer = Timer.start(meterRegistry);

    AtomicInteger successCounter = new AtomicInteger(0);
    List<ListenableFuture<?>> replicaEvictions =
        cacheNodeAssignments.stream()
            .filter(
                cacheNodeAssignment ->
                    shouldEvictReplica(
                        expireOlderThan, replicaMetadataBySnapshotId, cacheNodeAssignment))
            .map(
                (cacheNodeAssignment) -> {
                  ListenableFuture<?> future =
                      cacheNodeAssignmentStore.updateAssignmentState(
                          cacheNodeAssignment,
                          Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT);

                  addCallback(
                      future,
                      successCountingCallback(successCounter),
                      MoreExecutors.directExecutor());
                  return future;
                })
            .collect(Collectors.toUnmodifiableList());

    ListenableFuture<?> futureList = Futures.successfulAsList(replicaEvictions);
    try {
      futureList.get(futuresListTimeoutSecs, TimeUnit.SECONDS);
    } catch (Exception e) {
      futureList.cancel(true);
    }

    int successfulEvictions = successCounter.get();
    int failedEvictions = replicaEvictions.size() - successfulEvictions;

    evictSucceeded.increment(successfulEvictions);
    evictFailed.increment(failedEvictions);

    long evictionDuration = evictionTimer.stop(evictAssignmentTimer);
    LOG.info(
        "Completed assignment evictions - successfully marked {} assignments for eviction, failed to mark {} assignments for eviction in {} ms",
        successfulEvictions,
        failedEvictions,
        nanosToMillis(evictionDuration));

    return successfulEvictions;
  }

  /**
   * Persists cache node assignments in the cache node assignment store.
   *
   * @param cacheNodeAssignmentStore the store to persist cache node assignments
   * @param newAssignments a map of new assignments keyed by cache node ID
   * @param replicaSet the replica set for which to persist assignments
   * @param maxConcurrentAssignments the maximum amount of assignments to create for each cache node
   * @return the number of successful assignments
   */
  private static int persistAssignments(
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      Map<String, CacheNodeBin> newAssignments,
      String replicaSet,
      Map<String, ReplicaMetadata> replicasBySnapshotId,
      Map<String, Integer> cacheNodesByLoadingAssignments,
      int maxConcurrentAssignments) {
    int numCreated = 0;
    for (Map.Entry<String, CacheNodeBin> entry : newAssignments.entrySet()) {
      String cacheNodeId = entry.getKey();
      for (SnapshotMetadata snapshot :
          sortSnapshotsByReplicaCreationTime(
              entry.getValue().getSnapshots(), replicasBySnapshotId)) {
        if (cacheNodeId.startsWith(NEW_BIN_PREFIX)) {
          continue;
        }

        if (cacheNodesByLoadingAssignments.containsKey(cacheNodeId)
            && cacheNodesByLoadingAssignments.get(cacheNodeId) >= maxConcurrentAssignments) {
          continue;
        }

        CacheNodeAssignment newAssignment =
            new CacheNodeAssignment(
                UUID.randomUUID().toString(),
                cacheNodeId,
                snapshot.snapshotId,
                replicasBySnapshotId.get(snapshot.snapshotId).name,
                replicaSet,
                snapshot.sizeInBytesOnDisk,
                Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING);
        cacheNodeAssignmentStore.createSync(newAssignment);

        if (cacheNodesByLoadingAssignments.containsKey(cacheNodeId)) {
          cacheNodesByLoadingAssignments.compute(
              cacheNodeId, (key, loadingAssignments) -> loadingAssignments + 1);
        } else {
          cacheNodesByLoadingAssignments.put(cacheNodeId, 1);
        }

        numCreated++;
      }
    }
    return numCreated;
  }

  /**
   * Assigns snapshots to cache nodes using a first-fit packing algorithm. This method initializes
   * bins with existing cache nodes and existing assignments. Then, it assigns unassigned snapshots
   * to bins, putting them in new bins if required.
   *
   * @param cacheNodeAssignmentStore the store for cache node assignments
   * @param snapshotMetadataStore the store for snapshot metadata
   * @param currentAssignments the current assignments of cache nodes to snapshots
   * @param snapshotsToAssign the list of snapshots to be assigned
   * @param existingCacheNodes the list of existing cache nodes
   * @return a map of cache node bins keyed by cache node ID
   */
  @VisibleForTesting
  public static Map<String, CacheNodeBin> assign(
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      SnapshotMetadataStore snapshotMetadataStore,
      List<CacheNodeAssignment> currentAssignments,
      List<SnapshotMetadata> snapshotsToAssign,
      List<CacheNodeMetadata> existingCacheNodes) {
    int newBinsCreated = 0;
    Map<String, CacheNodeBin> cacheNodeBins = new HashMap<>();

    // Initialize bins with existing cache nodes
    for (CacheNodeMetadata cacheNodeMetadata : existingCacheNodes) {
      cacheNodeBins.put(
          cacheNodeMetadata.id, new CacheNodeBin(cacheNodeMetadata.nodeCapacityBytes));
    }

    // Add existing assignments to bins
    for (CacheNodeAssignment assignment : currentAssignments) {
      if (cacheNodeBins.containsKey(assignment.cacheNodeId)) {
        CacheNodeBin bin = cacheNodeBins.get(assignment.cacheNodeId);
        bin.subtractFromSize(
            snapshotMetadataStore.findSync(assignment.snapshotId).sizeInBytesOnDisk);
      } else {
        // delete this assignment since its cache node is no longer around
        cacheNodeAssignmentStore.deleteSync(assignment);
      }
    }

    List<CacheNodeBin> bins =
        cacheNodeBins.values().stream()
            .sorted(Comparator.comparingLong(CacheNodeBin::getRemainingCapacityBytes))
            .toList();

    // do first-fit packing for remaining snapshots
    for (SnapshotMetadata snapshot : snapshotsToAssign) {
      boolean assigned = false;
      for (CacheNodeBin cacheNodeBin : bins) {
        if (snapshot.sizeInBytesOnDisk <= cacheNodeBin.getRemainingCapacityBytes()) {
          cacheNodeBin.addSnapshot(snapshot);
          assigned = true;
          break;
        }
      }
      if (!assigned) {
        // if no bin can fit current item -> create new bin
        String newBinKey = String.format(NEW_BIN_PREFIX + "%s", newBinsCreated);
        CacheNodeBin newBin = new CacheNodeBin(snapshot.sizeInBytesOnDisk);

        newBin.addSnapshot(snapshot);
        cacheNodeBins.put(newBinKey, newBin);
        newBinsCreated++;
      }
    }

    return cacheNodeBins;
  }

  /**
   * Gets the list of unassigned snapshots by comparing the snapshots with replicas and the assigned
   * snapshots.
   *
   * <p>Unassigned snapshots are the set difference between snapshots with replicas and snapshots
   * with existing assignments. e.g. if a snapshot has a replica created, but no assignment to a
   * cache node, it is "unassigned".
   *
   * @param snapshotsWithReplicas the list of snapshots that have replicas
   * @param assignedSnapshots the list of already assigned snapshots
   * @return the list of unassigned snapshots
   */
  private static List<SnapshotMetadata> getUnassignedSnapshots(
      List<SnapshotMetadata> snapshotsWithReplicas, List<SnapshotMetadata> assignedSnapshots) {
    Set<SnapshotMetadata> unassignedSnapshots = Sets.newHashSet(snapshotsWithReplicas);
    unassignedSnapshots.removeAll(Sets.newHashSet(assignedSnapshots));
    return new ArrayList<>(unassignedSnapshots);
  }

  /**
   * Generates a map of snapshot metadata keyed by snapshot ID. This method retrieves a list of
   * snapshot metadata from the store and maps each snapshot ID to its corresponding metadata.
   *
   * @param snapshotMetadataStore the store containing snapshot metadata
   * @return a map of snapshot IDs to snapshot metadata
   */
  public static Map<String, SnapshotMetadata> snapshotMetadataBySnapshotId(
      SnapshotMetadataStore snapshotMetadataStore) {
    Map<String, SnapshotMetadata> snapshotIdsToMetadata = new HashMap<>();
    snapshotMetadataStore
        .listSync()
        .forEach(
            snapshotMetadata ->
                snapshotIdsToMetadata.putIfAbsent(snapshotMetadata.snapshotId, snapshotMetadata));

    return snapshotIdsToMetadata;
  }

  /**
   * Generates a map of replica metadata keyed by snapshot ID. This method takes a list of replica
   * metadata and maps each snapshot ID to its corresponding replica metadata.
   *
   * @param replicaMetadataList the list of replica metadata
   * @return a map of snapshot IDs to replica metadata
   */
  public static Map<String, ReplicaMetadata> replicaMetadataBySnapshotId(
      List<ReplicaMetadata> replicaMetadataList) {
    Map<String, ReplicaMetadata> snapshotIdsToMetadata = new HashMap<>();
    replicaMetadataList.forEach(
        replicaMetadata ->
            snapshotIdsToMetadata.putIfAbsent(replicaMetadata.snapshotId, replicaMetadata));

    return snapshotIdsToMetadata;
  }

  /**
   * Retrieves a list of snapshot metadata for the given snapshot IDs. This method takes a map of
   * snapshot metadata and a list of snapshot IDs, and returns a list of snapshot metadata
   * corresponding to those IDs.
   *
   * @param snapshotIdsToMetadata a map of snapshot IDs to snapshot metadata
   * @param snapshotIds the list of snapshot IDs to retrieve metadata for
   * @return a list of snapshot metadata corresponding to the given snapshot IDs
   */
  public static List<SnapshotMetadata> getSnapshotsFromIds(
      Map<String, SnapshotMetadata> snapshotIdsToMetadata, Set<String> snapshotIds) {
    List<SnapshotMetadata> snapshots = new ArrayList<>();
    for (String snapshotId : snapshotIds) {
      if (snapshotIdsToMetadata.containsKey(snapshotId)) {
        snapshots.add(snapshotIdsToMetadata.get(snapshotId));
      }
    }
    return snapshots;
  }

  /**
   * Checks if the cache slot should be evicted (currently live, and has an expiration in the past)
   */
  private static boolean shouldEvictReplica(
      Instant expireOlderThan,
      Map<String, ReplicaMetadata> replicaMetadataBySnapshotId,
      CacheNodeAssignment cacheNodeAssignment) {
    boolean shouldEvict =
        cacheNodeAssignment.state.equals(Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE)
            && replicaMetadataBySnapshotId.containsKey(cacheNodeAssignment.snapshotId)
            && replicaMetadataBySnapshotId.get(cacheNodeAssignment.snapshotId).expireAfterEpochMs
                < expireOlderThan.toEpochMilli();
    if (shouldEvict) {
      LOG.info("Should evict replica " + cacheNodeAssignment.snapshotId);
    } else {
      LOG.info("Should NOT evict replica with expireAfterMs of %s" + replicaMetadataBySnapshotId.get(cacheNodeAssignment.snapshotId).expireAfterEpochMs);
    }
    return shouldEvict;
  }

  private void assignmentListener(CacheNodeAssignment assignment) {
    if (assignment.state == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE) {
      runOneIteration();
    }
  }

  @VisibleForTesting
  public static List<SnapshotMetadata> sortSnapshotsByReplicaCreationTime(
      List<SnapshotMetadata> snapshotsToSort,
      Map<String, ReplicaMetadata> replicaMetadataBySnapshotId) {
    snapshotsToSort.sort(
        Comparator.comparingLong(
            snapshot -> replicaMetadataBySnapshotId.get(snapshot.snapshotId).createdTimeEpochMs));
    return snapshotsToSort;
  }
}

/**
 * CacheNodeBin class represents the assignments for a cache node. It tracks the total and remaining
 * capacity in bytes and stores snapshot IDs.
 */
class CacheNodeBin {
  private long remainingCapacityBytes;
  private final List<SnapshotMetadata> snapshots;
  private final long totalCapacityBytes;

  public CacheNodeBin(long totalCapacityBytes) {
    this.remainingCapacityBytes = totalCapacityBytes;
    this.snapshots = new ArrayList<>();
    this.totalCapacityBytes = totalCapacityBytes;
  }

  public long getTotalCapacityBytes() {
    return totalCapacityBytes;
  }

  public long getRemainingCapacityBytes() {
    return remainingCapacityBytes;
  }

  public void subtractFromSize(long sizeToSubtract) {
    this.remainingCapacityBytes -= sizeToSubtract;
  }

  public List<SnapshotMetadata> getSnapshots() {
    return snapshots;
  }

  public void addSnapshot(SnapshotMetadata snapshot) {
    this.snapshots.add(snapshot);
    subtractFromSize(snapshot.sizeInBytesOnDisk);
  }
}
