package com.slack.kaldb.clusterManager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;
import static com.slack.kaldb.util.FutureUtils.successCountingCallback;
import static com.slack.kaldb.util.TimeUtils.nanosToMillis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.metadata.cache.CacheSlotMetadata;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
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
  private final KaldbConfigs.ManagerConfig managerConfig;
  private final MeterRegistry meterRegistry;

  @VisibleForTesting protected int futuresListTimeoutSecs = DEFAULT_ZK_TIMEOUT_SECS;

  public static final String REPLICA_ASSIGN_SUCCEEDED = "replica_assign_succeeded";
  public static final String REPLICA_ASSIGN_FAILED = "replica_assign_failed";
  public static final String REPLICA_ASSIGN_AVAILABLE_CAPACITY =
      "replica_assign_available_capacity";
  public static final String REPLICA_ASSIGN_TIMER = "replica_assign_timer";

  protected final Counter replicaAssignSucceeded;
  protected final Counter replicaAssignFailed;
  protected final AtomicInteger replicaAssignAvailableCapacity;
  private final Timer replicaAssignTimer;

  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private ScheduledFuture<?> pendingTask;

  public ReplicaAssignmentService(
      CacheSlotMetadataStore cacheSlotMetadataStore,
      ReplicaMetadataStore replicaMetadataStore,
      KaldbConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {
    this.cacheSlotMetadataStore = cacheSlotMetadataStore;
    this.replicaMetadataStore = replicaMetadataStore;
    this.managerConfig = managerConfig;
    this.meterRegistry = meterRegistry;

    checkArgument(managerConfig.getEventAggregationSecs() > 0, "eventAggregationSecs must be > 0");
    // schedule configs checked as part of the AbstractScheduledService

    replicaAssignSucceeded = meterRegistry.counter(REPLICA_ASSIGN_SUCCEEDED);
    replicaAssignFailed = meterRegistry.counter(REPLICA_ASSIGN_FAILED);
    replicaAssignAvailableCapacity =
        meterRegistry.gauge(REPLICA_ASSIGN_AVAILABLE_CAPACITY, new AtomicInteger(0));
    replicaAssignTimer = meterRegistry.timer(REPLICA_ASSIGN_TIMER);
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
    cacheSlotMetadataStore.addListener(this::runOneIteration);
    replicaMetadataStore.addListener(this::runOneIteration);
  }

  @Override
  protected void shutDown() throws Exception {
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
  protected int assignReplicasToCacheSlots() {
    Timer.Sample assignmentTimer = Timer.start(meterRegistry);

    List<CacheSlotMetadata> availableCacheSlots =
        cacheSlotMetadataStore
            .getCached()
            .stream()
            .filter(
                cacheSlotMetadata ->
                    cacheSlotMetadata.cacheSlotState.equals(
                        Metadata.CacheSlotMetadata.CacheSlotState.FREE))
            .collect(Collectors.toList());

    // Force a shuffle of the available slots, to reduce the chance of a single cache node getting
    // assigned chunks that matches all recent queries. This should help balance out the load
    // across all available hosts.
    Collections.shuffle(availableCacheSlots);

    Set<String> assignedReplicaIds =
        cacheSlotMetadataStore
            .getCached()
            .stream()
            .filter(cacheSlotMetadata -> !cacheSlotMetadata.replicaId.isEmpty())
            .map(cacheSlotMetadata -> cacheSlotMetadata.replicaId)
            .collect(Collectors.toUnmodifiableSet());

    long nowMilli = Instant.now().toEpochMilli();
    List<String> replicaIdsToAssign =
        replicaMetadataStore
            .getCached()
            .stream()
            // only assign replicas that are not expired, and not already assigned
            .filter(
                replicaMetadata ->
                    replicaMetadata.expireAfterEpochMs > nowMilli
                        && !assignedReplicaIds.contains(replicaMetadata.name))
            // sort the list by the newest replicas first, in case we run out of available slots
            .sorted(Comparator.comparingLong(ReplicaMetadata::getCreatedTimeEpochMs).reversed())
            .map(replicaMetadata -> replicaMetadata.name)
            .collect(Collectors.toUnmodifiableList());

    // Report either a positive value (excess capacity) or a negative value (insufficient capacity)
    replicaAssignAvailableCapacity.set(availableCacheSlots.size() - replicaIdsToAssign.size());

    if (replicaIdsToAssign.size() > availableCacheSlots.size()) {
      LOG.warn(
          "Insufficient cache slots to assign replicas, wanted {} slots but had {} replicas",
          replicaIdsToAssign.size(),
          availableCacheSlots.size());
    } else if (replicaIdsToAssign.size() == 0) {
      LOG.info("No replicas found requiring assignment");
      assignmentTimer.stop(replicaAssignTimer);
      return 0;
    }

    AtomicInteger successCounter = new AtomicInteger(0);
    List<ListenableFuture<?>> replicaAssignments =
        Streams.zip(
                replicaIdsToAssign.stream(),
                availableCacheSlots.stream(),
                (replicaId, availableCacheSlot) -> {
                  CacheSlotMetadata assignedCacheSlot =
                      new CacheSlotMetadata(
                          availableCacheSlot.name,
                          Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
                          replicaId,
                          Instant.now().toEpochMilli(),
                          availableCacheSlot.supportedIndexTypes);

                  ListenableFuture<?> future = cacheSlotMetadataStore.update(assignedCacheSlot);
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

    replicaAssignSucceeded.increment(successfulAssignments);
    replicaAssignFailed.increment(failedAssignments);

    long assignmentDuration = assignmentTimer.stop(replicaAssignTimer);
    LOG.info(
        "Completed replica assignment - successfully assigned {} replicas, failed to assign {} replicas in {} ms",
        successfulAssignments,
        failedAssignments,
        nanosToMillis(assignmentDuration));

    return successfulAssignments;
  }
}
