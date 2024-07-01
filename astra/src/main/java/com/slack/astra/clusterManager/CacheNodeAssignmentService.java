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
import com.slack.astra.metadata.hpa.HpaMetricMetadata;
import com.slack.astra.metadata.hpa.HpaMetricMetadataStore;
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
import java.util.HashMap;
import java.util.HashSet;
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

/**
 * Service responsible for managing the assignment & cleanup of replicas to cache nodes.
 *
 * <p>Periodically assign replicas to cache nodes based on the configured replica sets. It fetches
 * metadata from various stores, performs the assignments, and calculates HPA (Horizontal Pod
 * Autoscaler) metrics for capacity management. Metrics are tracked for assignment and eviction
 * operations.
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
  private final HpaMetricMetadataStore hpaMetricMetadataStore;

  private final AstraMetadataStoreChangeListener<CacheNodeMetadata> cacheNodeListener =
      (cacheNodeMetadata) -> runOneIteration();
  private final AstraMetadataStoreChangeListener<ReplicaMetadata> replicaListener =
      (replicaMetadata) -> runOneIteration();

  protected static final Logger LOG = LoggerFactory.getLogger(ReplicaCreationService.class);
  private static final String NEW_BIN_PREFIX = "NEW_";
  protected static final String CACHE_HPA_METRIC_NAME = "hpa_capacity_demand_factor_%s";
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
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      HpaMetricMetadataStore hpaMetricMetadataStore) {
    this.managerConfig = managerConfig;
    this.meterRegistry = meterRegistry;
    this.replicaMetadataStore = replicaMetadataStore;
    this.cacheNodeMetadataStore = cacheNodeMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.cacheNodeAssignmentStore = cacheNodeAssignmentStore;
    this.hpaMetricMetadataStore = hpaMetricMetadataStore;

    assignmentCreateSucceeded = meterRegistry.counter(ASSIGNMENT_CREATE_SUCCEEDED);
    assignmentCreateFailed = meterRegistry.counter(ASSIGNMENT_CREATE_FAILED);
    assignmentCreateTimer = meterRegistry.timer(ASSIGNMENT_CREATE_TIMER);
    evictAssignmentTimer = meterRegistry.timer(EVICT_ASSIGNMENT_TIMER);
    evictSucceeded = meterRegistry.counter(EVICT_SUCCEEDED);
    evictFailed = meterRegistry.counter(EVICT_FAILED);
  }

  @Override
  protected void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executorService.schedule(
              this::assignReplicasToCacheNodes,
              managerConfig.getCacheNodeAssignmentServiceConfig().getSchedulePeriodMins(),
              TimeUnit.SECONDS);
    } else {
      LOG.info(
          "Cache node assignment task already scheduled, will run in {} ms",
          pendingTask.getDelay(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting cache node assignment service");
    cacheNodeMetadataStore.addListener(cacheNodeListener);
    replicaMetadataStore.addListener(replicaListener);
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

      markAssignmentsForEviction(
          currentAssignments, replicaMetadataBySnapshotId(replicas), Instant.now());

      Map<String, SnapshotMetadata> snapshotIdsToMetadata =
          snapshotMetadataBySnapshotId(snapshotMetadataStore);

      List<SnapshotMetadata> assignedSnapshots =
          getSnapshotsFromIds(
              snapshotIdsToMetadata, currentAssignments.stream().map(x -> x.snapshotId).toList());
      List<SnapshotMetadata> snapshotsWithReplicas =
          getSnapshotsFromIds(
              snapshotIdsToMetadata, replicas.stream().map(x -> x.snapshotId).toList());
      List<SnapshotMetadata> unassignedSnapshots =
          getUnassignedSnapshots(snapshotsWithReplicas, assignedSnapshots);

      Map<String, CacheNodeBin> newAssignments =
          assign(
              cacheNodeAssignmentStore,
              snapshotMetadataStore,
              currentAssignments,
              unassignedSnapshots,
              cacheNodes);
      int successfulAssignments =
          createAssignments(cacheNodeAssignmentStore, newAssignments, replicaSet);
      int failedAssignments = newAssignments.size() - successfulAssignments;

      calculateAndPersistHpaMetric(hpaMetricMetadataStore, replicaSet, newAssignments, cacheNodes);

      assignmentCreateSucceeded.increment(successfulAssignments);
      assignmentCreateFailed.increment(failedAssignments);

      long evictionDuration = assignmentTimer.stop(assignmentCreateTimer);
      LOG.info(
          "Completed cache node assignments - successfully assigned {} replicas, failed to assign {} replicas in {} ms",
          successfulAssignments,
          failedAssignments,
          nanosToMillis(evictionDuration));
    }
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

    assignmentCreateSucceeded.increment(successfulEvictions);
    assignmentCreateFailed.increment(failedEvictions);

    long evictionDuration = evictionTimer.stop(evictAssignmentTimer);
    LOG.info(
        "Completed assignment evictions - successfully marked {} assignments for eviction, failed to mark {} assignments for eviction in {} ms",
        successfulEvictions,
        failedEvictions,
        nanosToMillis(evictionDuration));

    return successfulEvictions;
  }

  /**
   * Calculates and persists the HPA (Horizontal Pod Autoscaler) metric based on new assignments.
   * This method calculates the demand factor from new assignments and persists it in the HPA metric
   * metadata store.
   *
   * @param hpaMetricMetadataStore the store to persist HPA metrics
   * @param replicaSet the replica set for which to calculate the HPA metric
   * @param newAssignments the new assignments of cache nodes to snapshots
   * @param cacheNodes the list of cache nodes
   */
  private static void calculateAndPersistHpaMetric(
      HpaMetricMetadataStore hpaMetricMetadataStore,
      String replicaSet,
      Map<String, CacheNodeBin> newAssignments,
      List<CacheNodeMetadata> cacheNodes) {
    double demandFactor =
        calculateHpaValueFromNewAssignments(
            newAssignments, cacheNodes.stream().mapToLong(node -> node.nodeCapacityBytes).sum());
    persistCacheConfig(hpaMetricMetadataStore, replicaSet, demandFactor);
  }

  /**
   * Persists cache node assignments in the cache node assignment store.
   *
   * @param cacheNodeAssignmentStore the store to persist cache node assignments
   * @param newAssignments a map of new assignments keyed by cache node ID
   * @param replicaSet the replica set for which to persist assignments
   * @return the number of successful assignments
   */
  private static int createAssignments(
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      Map<String, CacheNodeBin> newAssignments,
      String replicaSet) {
    int numAssigned = 0;
    for (Map.Entry<String, CacheNodeBin> entry : newAssignments.entrySet()) {
      String cacheNodeId = entry.getKey();
      for (String snapshotId : entry.getValue().getSnapshotIds()) {
        if (cacheNodeId.startsWith(NEW_BIN_PREFIX)) {
          continue;
        }
        CacheNodeAssignment newAssignment =
            new CacheNodeAssignment(
                UUID.randomUUID().toString(),
                cacheNodeId,
                snapshotId,
                replicaSet,
                Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING);
        cacheNodeAssignmentStore.createSync(newAssignment);
        numAssigned++;
      }
    }
    return numAssigned;
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
    return new ArrayList<>(
        Sets.difference(
            Sets.newHashSet(snapshotsWithReplicas), Sets.newHashSet(assignedSnapshots)));
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
          cacheNodeMetadata.name, new CacheNodeBin(cacheNodeMetadata.nodeCapacityBytes));
    }

    // Add existing assignments to bins
    for (CacheNodeAssignment assignment : currentAssignments) {
      if (cacheNodeBins.containsKey(assignment.cacheNodeId)) {
        CacheNodeBin bin = cacheNodeBins.get(assignment.cacheNodeId);
        bin.addSnapshot(assignment.snapshotId);
        bin.subtractFromSize(
            snapshotMetadataStore.findSync(assignment.snapshotId).sizeInBytesOnDisk);
      } else {
        cacheNodeAssignmentStore.deleteSync(assignment);
        snapshotsToAssign.add(snapshotMetadataStore.findSync(assignment.snapshotId));
      }
    }

    // do first-fit packing for remaining snapshots
    for (SnapshotMetadata snapshot : snapshotsToAssign) {
      boolean assigned = false;
      for (Map.Entry<String, CacheNodeBin> binEntry : cacheNodeBins.entrySet()) {
        CacheNodeBin cacheNodeBin = binEntry.getValue();
        if (snapshot.sizeInBytesOnDisk <= cacheNodeBin.getRemainingCapacityBytes()) {
          cacheNodeBin.addSnapshot(snapshot.snapshotId);
          cacheNodeBin.subtractFromSize(snapshot.sizeInBytesOnDisk);
          assigned = true;
          break;
        }
      }
      if (!assigned) {
        // if no bin can fit current item -> create new bin
        String newBinKey = String.format(NEW_BIN_PREFIX + "%s", newBinsCreated);
        CacheNodeBin newBin = new CacheNodeBin(snapshot.sizeInBytesOnDisk);

        newBin.subtractFromSize(snapshot.sizeInBytesOnDisk);
        newBin.addSnapshot(snapshot.snapshotId);
        cacheNodeBins.put(newBinKey, newBin);
        newBinsCreated++;
      }
    }

    return cacheNodeBins;
  }

  /**
   * Generates a map of snapshot metadata keyed by snapshot ID. This method retrieves a list of
   * snapshot metadata from the store and maps each snapshot ID to its corresponding metadata.
   *
   * @param snapshotMetadataStore the store containing snapshot metadata
   * @return a map of snapshot IDs to snapshot metadata
   */
  private static Map<String, SnapshotMetadata> snapshotMetadataBySnapshotId(
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
  private static Map<String, ReplicaMetadata> replicaMetadataBySnapshotId(
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
  private static List<SnapshotMetadata> getSnapshotsFromIds(
      Map<String, SnapshotMetadata> snapshotIdsToMetadata, List<String> snapshotIds) {
    // use snapshotID to get size of each replica
    List<SnapshotMetadata> snapshots = new ArrayList<>();
    for (String snapshotId : snapshotIds) {
      if (snapshotIdsToMetadata.containsKey(snapshotId)) {
        snapshots.add(snapshotIdsToMetadata.get(snapshotId));
      }
    }
    return snapshots;
  }

  /**
   * Calculates the HPA (Horizontal Pod Autoscaler) value based on new assignments. This method
   * computes the demand factor as the ratio of total bytes requiring assignment to the total
   * capacity of cache nodes.
   *
   * @param newAssignments a map of new assignments keyed by cache node ID
   * @param totalCacheNodeCapacityBytes the total capacity of all cache nodes in bytes
   * @return the calculated HPA value
   */
  private static double calculateHpaValueFromNewAssignments(
      Map<String, CacheNodeBin> newAssignments, long totalCacheNodeCapacityBytes) {
    long totalBytesRequiringAssignment =
        newAssignments.values().stream()
            .mapToLong(
                cacheNodeBin ->
                    cacheNodeBin.getTotalCapacityBytes() - cacheNodeBin.getRemainingCapacityBytes())
            .sum();

    return (double) totalBytesRequiringAssignment / totalCacheNodeCapacityBytes;
  }

  /** Updates or inserts an (ephemeral) HPA metric for the cache nodes. This is NOT threadsafe. */
  private static void persistCacheConfig(
      HpaMetricMetadataStore hpaMetricMetadataStore, String replicaSet, Double demandFactor) {
    String key = String.format(CACHE_HPA_METRIC_NAME, replicaSet);
    if (hpaMetricMetadataStore.hasSync(key)) {
      hpaMetricMetadataStore.updateSync(
          new HpaMetricMetadata(key, Metadata.HpaMetricMetadata.NodeRole.CACHE, demandFactor));
    } else {
      hpaMetricMetadataStore.createSync(
          new HpaMetricMetadata(key, Metadata.HpaMetricMetadata.NodeRole.CACHE, demandFactor));
    }
  }

  /**
   * Checks if the cache slot should be evicted (currently live, and has an expiration in the past)
   */
  private static boolean shouldEvictReplica(
      Instant expireOlderThan,
      Map<String, ReplicaMetadata> replicaMetadataByReplicaId,
      CacheNodeAssignment cacheNodeAssignment) {
    return cacheNodeAssignment.state.equals(
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE)
        && replicaMetadataByReplicaId.containsKey(cacheNodeAssignment.snapshotId)
        && replicaMetadataByReplicaId.get(cacheNodeAssignment.snapshotId).expireAfterEpochMs
            < expireOlderThan.toEpochMilli();
  }
}

/**
 * CacheNodeBin class represents the assignments for a cache node. It tracks the total and remaining
 * capacity in bytes and stores snapshot IDs.
 */
class CacheNodeBin {
  private long remainingCapacityBytes;
  private final Set<String> snapshotIds;
  private final long totalCapacityBytes;

  public CacheNodeBin(long totalCapacityBytes) {
    this.remainingCapacityBytes = totalCapacityBytes;
    this.snapshotIds = new HashSet<>();
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

  public Set<String> getSnapshotIds() {
    return snapshotIds;
  }

  public void addSnapshot(String snapshotId) {
    this.snapshotIds.add(snapshotId);
  }
}
