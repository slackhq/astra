package com.slack.astra.clusterManager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.astra.server.AstraConfig.DEFAULT_ZK_TIMEOUT_SECS;
import static com.slack.astra.util.FutureUtils.successCountingCallback;
import static com.slack.astra.util.TimeUtils.nanosToMillis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.astra.metadata.cache.CacheSlotMetadata;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.replica.ReplicaMetadata;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The replica assignment service watches for changes in the available cache slots and replicas
 * requiring assignments, and attempts to assign replicas to available slots. In the event there are
 * no available slots a failure will be noted and the assignment will be retried on the following
 * run.
 */
public class ReplicaAssignmentService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaAssignmentService.class);

  private final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final ReplicaMetadataStore replicaMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final AstraConfigs.ManagerConfig managerConfig;
  private final MeterRegistry meterRegistry;

  @VisibleForTesting protected int futuresListTimeoutSecs = DEFAULT_ZK_TIMEOUT_SECS;

  private final int maxConcurrentAssignmentsPerNode;

  public static final String REPLICA_ASSIGN_SUCCEEDED = "replica_assign_succeeded";
  public static final String REPLICA_ASSIGN_PENDING = "replica_assign_pending";
  public static final String REPLICA_ASSIGN_FAILED = "replica_assign_failed";
  public static final String REPLICA_ASSIGN_AVAILABLE_CAPACITY =
      "replica_assign_available_capacity";
  public static final String REPLICA_ASSIGN_TIMER = "replica_assign_timer";

  private final Counter.Builder replicaAssignSucceeded;
  private final Counter.Builder replicaAssignFailed;
  private final Timer.Builder replicaAssignTimer;

  private final Map<String, AtomicInteger> replicaAssignAvailableCapacity =
      new ConcurrentHashMap<>();

  private final Map<String, AtomicInteger> replicaAssignPending = new ConcurrentHashMap<>();

  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private ScheduledFuture<?> pendingTask;

  private final AstraMetadataStoreChangeListener<CacheSlotMetadata> cacheSlotListener =
      (cacheSlotMetadata) -> runOneIteration();
  private final AstraMetadataStoreChangeListener<ReplicaMetadata> replicaListener =
      (replicaMetadata) -> runOneIteration();

  public ReplicaAssignmentService(
      CacheSlotMetadataStore cacheSlotMetadataStore,
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      AstraConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {
    this.cacheSlotMetadataStore = cacheSlotMetadataStore;
    this.replicaMetadataStore = replicaMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.managerConfig = managerConfig;
    this.meterRegistry = meterRegistry;

    checkArgument(
        managerConfig.getReplicaAssignmentServiceConfig().getMaxConcurrentPerNode() > 0,
        "maxConcurrentPerNode must be > 0");
    checkArgument(
        managerConfig.getReplicaAssignmentServiceConfig().getReplicaSetsCount() > 0,
        "replicaSets must not be empty");
    checkArgument(managerConfig.getEventAggregationSecs() > 0, "eventAggregationSecs must be > 0");
    // schedule configs checked as part of the AbstractScheduledService

    this.maxConcurrentAssignmentsPerNode =
        managerConfig.getReplicaAssignmentServiceConfig().getMaxConcurrentPerNode();

    this.replicaAssignSucceeded = Counter.builder(REPLICA_ASSIGN_SUCCEEDED);
    this.replicaAssignFailed = Counter.builder(REPLICA_ASSIGN_FAILED);
    this.replicaAssignTimer = Timer.builder(REPLICA_ASSIGN_TIMER);
  }

  @Override
  protected synchronized void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executorService.schedule(
              this::assignReplicasToCacheSlots,
              managerConfig.getEventAggregationSecs(),
              TimeUnit.SECONDS);
    } else {
      LOG.debug(
          "Replica assignment already queued for execution, will run in {} ms",
          pendingTask.getDelay(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        managerConfig.getScheduleInitialDelayMins(),
        managerConfig.getReplicaAssignmentServiceConfig().getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting replica assignment service");
    cacheSlotMetadataStore.addListener(cacheSlotListener);
    replicaMetadataStore.addListener(replicaListener);
  }

  @Override
  protected void shutDown() throws Exception {
    cacheSlotMetadataStore.removeListener(cacheSlotListener);
    replicaMetadataStore.removeListener(replicaListener);
    executorService.shutdown();
    LOG.info("Closed replica assignment service");
  }

  /**
   * Assigns replicas to available cache slots, up to the configured replica lifespan min
   * configuration. Replicas will be assigned with the most recently created first in descending
   * order. In the event that more replicas than slots exist this ensures the most recent replicas
   * are preferred. No preference is given to specific cache slots, which may result in over/under
   * utilization of specific cache slots.
   *
   * <p>If this method fails to successfully assign all the replicas needing slot assignment, the
   * following iteration of this method would attempt to re-assign these until there are no more
   * available replicas to assign.
   *
   * @return The count of successfully assigned cache slots
   */
  @SuppressWarnings("UnstableApiUsage")
  protected Map<String, Integer> assignReplicasToCacheSlots() {
    Map<String, Integer> assignments = new HashMap<>();

    for (String replicaSet :
        managerConfig.getReplicaAssignmentServiceConfig().getReplicaSetsList()) {
      Timer.Sample assignmentTimer = Timer.start(meterRegistry);

      List<CacheSlotMetadata> availableCacheSlots =
          cacheSlotMetadataStore.listSync().stream()
              .filter(
                  cacheSlotMetadata ->
                      cacheSlotMetadata.cacheSlotState.equals(
                              Metadata.CacheSlotMetadata.CacheSlotState.FREE)
                          && cacheSlotMetadata.replicaSet.equals(replicaSet))
              .toList();

      // only allow N pending assignments per host at once
      List<CacheSlotMetadata> assignableCacheSlots =
          cacheSlotMetadataStore.listSync().stream()
              .filter(cacheSlotMetadata -> cacheSlotMetadata.replicaSet.equals(replicaSet))
              .collect(Collectors.groupingBy(CacheSlotMetadata::getHostname))
              .values()
              .stream()
              .flatMap(
                  (cacheSlotsPerHost) -> {
                    int currentlyAssignedOrLoading =
                        cacheSlotsPerHost.stream()
                            .filter(
                                cacheSlotMetadata ->
                                    cacheSlotMetadata.cacheSlotState.equals(
                                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED)
                                        || cacheSlotMetadata.cacheSlotState.equals(
                                            Metadata.CacheSlotMetadata.CacheSlotState.LOADING))
                            .toList()
                            .size();

                    return cacheSlotsPerHost.stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                        .limit(
                            Math.max(
                                0, maxConcurrentAssignmentsPerNode - currentlyAssignedOrLoading));
                  })
              .collect(Collectors.toList());

      // Force a shuffle of the assignable slots, to reduce the chance of a single cache node
      // getting assigned chunks that matches all recent queries. This should help balance
      // out the load across all available hosts.
      Collections.shuffle(assignableCacheSlots);

      Set<String> assignedReplicaIds =
          cacheSlotMetadataStore.listSync().stream()
              .filter(
                  cacheSlotMetadata ->
                      !cacheSlotMetadata.replicaId.isEmpty()
                          && cacheSlotMetadata.replicaSet.equals(replicaSet))
              .map(cacheSlotMetadata -> cacheSlotMetadata.replicaId)
              .collect(Collectors.toUnmodifiableSet());

      long nowMilli = Instant.now().toEpochMilli();
      Set<String> expiredSnapshotIds = this.snapshotMetadataStore.listSync().stream()
              .filter(
                      snapshotMetadata -> snapshotMetadata.endTimeEpochMs <= nowMilli
              ).map(SnapshotMetadata::getName)
              .collect(Collectors.toUnmodifiableSet());

      List<String> replicaIdsToAssign =
          replicaMetadataStore.listSync().stream()
              // only assign replicas that are not expired, and not already assigned
              .filter(
                  replicaMetadata ->
                      replicaMetadata.expireAfterEpochMs > nowMilli
                          && !assignedReplicaIds.contains(replicaMetadata.name)
                          && replicaMetadata.getReplicaSet().equals(replicaSet))
                  // REMOVEME: After the clusters are back in a good state we should be able to remove this
//                  .filter(replicaMetadata -> {
//                    Instant sevenDaysAgo = Instant.now().minusSeconds(5 * 60 * 60 * 24);
//                    return replicaMetadata.createdTimeEpochMs >= sevenDaysAgo.toEpochMilli();
//                  })
                  .filter(replicaMetadata -> !expiredSnapshotIds.contains(replicaMetadata.snapshotId))
                  // REMOVEME: After the clusters are back in a good state we should be able to remove this
              // sort the list by the newest replicas first, in case we run out of available slots
              .sorted(Comparator.comparingLong(ReplicaMetadata::getCreatedTimeEpochMs).reversed())
              .map(replicaMetadata -> replicaMetadata.name)
              .toList();

      // Report either a positive value (excess capacity) or a negative value (insufficient
      // capacity)
      replicaAssignAvailableCapacity.putIfAbsent(
          replicaSet,
          meterRegistry.gauge(
              REPLICA_ASSIGN_AVAILABLE_CAPACITY,
              List.of(Tag.of("replicaSet", replicaSet)),
              new AtomicInteger(0)));
      replicaAssignAvailableCapacity
          .get(replicaSet)
          .set(availableCacheSlots.size() - replicaIdsToAssign.size());
      if (replicaIdsToAssign.size() > availableCacheSlots.size()) {
        LOG.warn(
            "Insufficient cache slots to assign replicas for replicaSet {}, wanted {} slots but had {} replicas",
            replicaSet,
            replicaIdsToAssign.size(),
            availableCacheSlots.size());
      } else if (replicaIdsToAssign.size() == 0) {
        LOG.info(
            "No replicas found requiring assignment in replicaSet {}, had {} available slots with {} replicas assigned",
            replicaSet,
            availableCacheSlots.size(),
            assignedReplicaIds.size());
        assignmentTimer.stop(
            replicaAssignTimer.tag("replicaSet", replicaSet).register(meterRegistry));
        assignments.put(replicaSet, 0);
        continue;
      }

      // report the number of things that need assigning, but aren't getting assigned in this pass
      // due to the maxConcurrentAssignmentsPerNode limit
      replicaAssignPending.putIfAbsent(
          replicaSet,
          meterRegistry.gauge(
              REPLICA_ASSIGN_PENDING,
              List.of(Tag.of("replicaSet", replicaSet)),
              new AtomicInteger(0)));
      replicaAssignPending
          .get(replicaSet)
          .set(Math.max(0, replicaIdsToAssign.size() - assignableCacheSlots.size()));

      AtomicInteger successCounter = new AtomicInteger(0);
      List<ListenableFuture<?>> replicaAssignments =
          Streams.zip(
                  replicaIdsToAssign.stream(),
                  assignableCacheSlots.stream(),
                  (replicaId, availableCacheSlot) -> {
                    ListenableFuture<?> future =
                        cacheSlotMetadataStore.updateCacheSlotStateStateWithReplicaId(
                            availableCacheSlot,
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
                            replicaId);
                    addCallback(
                        future,
                        successCountingCallback(successCounter),
                        MoreExecutors.directExecutor());
                    return future;
                  })
              .collect(Collectors.toList());

      ListenableFuture<?> futureList = Futures.successfulAsList(replicaAssignments);
      try {
        futureList.get(futuresListTimeoutSecs, TimeUnit.SECONDS);
      } catch (Exception e) {
        futureList.cancel(true);
      }

      int successfulAssignments = successCounter.get();
      int failedAssignments = replicaAssignments.size() - successfulAssignments;

      replicaAssignSucceeded
          .tag("replicaSet", replicaSet)
          .register(meterRegistry)
          .increment(successfulAssignments);
      replicaAssignFailed
          .tag("replicaSet", replicaSet)
          .register(meterRegistry)
          .increment(failedAssignments);

      long assignmentDuration =
          assignmentTimer.stop(
              replicaAssignTimer.tag("replicaSet", replicaSet).register(meterRegistry));
      LOG.info(
          "Completed replica assignment for replicaSet {} - successfully assigned {} replicas, failed to assign {} replicas in {} ms",
          replicaSet,
          successfulAssignments,
          failedAssignments,
          nanosToMillis(assignmentDuration));

      assignments.put(replicaSet, successfulAssignments);
    }
    return assignments;
  }
}
