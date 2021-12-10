package com.slack.kaldb.clusterManager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.kaldb.config.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;
import static com.slack.kaldb.util.FutureUtils.successCountingCallback;

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
import java.time.temporal.ChronoUnit;
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
 * The cache slot assignment service watches for changes in the available cache slots and replicas
 * requiring assignments, and attempts to assign replicas to available slots. In the event there are
 * no available slots a failure will be noted and the assignment will be retried on the following
 * run.
 */
public class CacheSlotAssignmentService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(CacheSlotAssignmentService.class);

  private final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final ReplicaMetadataStore replicaMetadataStore;
  private final KaldbConfigs.ManagerConfig managerConfig;
  private final MeterRegistry meterRegistry;

  @VisibleForTesting protected int futuresListTimeoutSecs = DEFAULT_ZK_TIMEOUT_SECS;

  public static final String SLOT_ASSIGN_SUCCEEDED = "cache_slot_assign_succeeded";
  public static final String SLOT_ASSIGN_FAILED = "cache_slot_assign_failed";
  public static final String SLOT_ASSIGN_INSUFFICIENT_CAPACITY =
      "cache_slot_assign_insufficient_capacity";
  public static final String SLOT_ASSIGN_TIMER = "cache_slot_assign_timer";

  protected final Counter slotAssignSucceeded;
  protected final Counter slotAssignFailed;
  protected final Counter slotAssignInsufficientCapacity;
  private final Timer slotAssignTimer;

  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private ScheduledFuture<?> pendingTask;

  public CacheSlotAssignmentService(
      CacheSlotMetadataStore cacheSlotMetadataStore,
      ReplicaMetadataStore replicaMetadataStore,
      KaldbConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {
    this.cacheSlotMetadataStore = cacheSlotMetadataStore;
    this.replicaMetadataStore = replicaMetadataStore;
    this.managerConfig = managerConfig;
    this.meterRegistry = meterRegistry;

    checkArgument(managerConfig.getEventAggregationSecs() > 0, "eventAggregationSecs must be > 0");
    checkArgument(
        managerConfig.getReplicaEvictionServiceConfig().getReplicaLifespanMins() > 0,
        "replicaLifespanMins must be > 0");
    // schedule configs checked as part of the AbstractScheduledService

    slotAssignSucceeded = meterRegistry.counter(SLOT_ASSIGN_SUCCEEDED);
    slotAssignFailed = meterRegistry.counter(SLOT_ASSIGN_FAILED);
    slotAssignInsufficientCapacity = meterRegistry.counter(SLOT_ASSIGN_INSUFFICIENT_CAPACITY);
    slotAssignTimer = meterRegistry.timer(SLOT_ASSIGN_TIMER);
  }

  @Override
  protected synchronized void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executorService.schedule(
              this::assignCacheSlotsToReplicas,
              managerConfig.getEventAggregationSecs(),
              TimeUnit.SECONDS);
    } else {
      LOG.debug(
          "Cache slot assignment already queued for execution, will run in {} ms",
          pendingTask.getDelay(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        managerConfig.getScheduleInitialDelayMins(),
        managerConfig.getCacheSlotAssignmentServiceConfig().getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting cache slot assignment service");
    cacheSlotMetadataStore.addListener(this::runOneIteration);
    replicaMetadataStore.addListener(this::runOneIteration);
  }

  @Override
  protected void shutDown() throws Exception {
    executorService.shutdown();
    LOG.info("Closed cache assignment service");
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
  protected int assignCacheSlotsToReplicas() {
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

    Set<String> assignedReplicaIds =
        cacheSlotMetadataStore
            .getCached()
            .stream()
            .filter(cacheSlotMetadata -> !cacheSlotMetadata.replicaId.isEmpty())
            .map(cacheSlotMetadata -> cacheSlotMetadata.replicaId)
            .collect(Collectors.toUnmodifiableSet());

    long cutoffCreatedTime =
        Instant.now()
            .minus(
                managerConfig.getReplicaEvictionServiceConfig().getReplicaLifespanMins(),
                ChronoUnit.MINUTES)
            .toEpochMilli();
    List<String> replicaIdsToAssign =
        replicaMetadataStore
            .getCached()
            .stream()
            // only replicas created in the last X mins, that are not already assigned
            .filter(
                replicaMetadata ->
                    replicaMetadata.createdTimeUtc > cutoffCreatedTime
                        && !assignedReplicaIds.contains(replicaMetadata.name))
            // sort the list by the newest replicas first, in case we run out of available slots
            .sorted(Comparator.comparingLong(ReplicaMetadata::getCreatedTimeUtc))
            .map(replicaMetadata -> replicaMetadata.name)
            .collect(Collectors.toUnmodifiableList());

    if (replicaIdsToAssign.size() > availableCacheSlots.size()) {
      LOG.warn(
          "Insufficient cache slots to assign replicas, wanted {} slots but had {} replicas",
          replicaIdsToAssign.size(),
          availableCacheSlots.size());
      slotAssignInsufficientCapacity.increment(
          replicaIdsToAssign.size() - availableCacheSlots.size());
    } else if (replicaIdsToAssign.size() == 0) {
      LOG.info("No replicas found requiring assignment");
      assignmentTimer.stop(slotAssignTimer);
      return 0;
    }

    AtomicInteger successCounter = new AtomicInteger(0);
    List<ListenableFuture<?>> cacheSlotAssignments =
        Streams.zip(
                replicaIdsToAssign.stream(),
                availableCacheSlots.stream(),
                (replicaId, availableCacheSlot) -> {
                  CacheSlotMetadata assignedCacheSlot =
                      new CacheSlotMetadata(
                          availableCacheSlot.name,
                          Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
                          replicaId,
                          Instant.now().toEpochMilli());

                  ListenableFuture<?> future = cacheSlotMetadataStore.update(assignedCacheSlot);
                  addCallback(
                      future,
                      successCountingCallback(successCounter),
                      MoreExecutors.directExecutor());
                  return future;
                })
            .collect(Collectors.toList());

    ListenableFuture<?> futureList = Futures.successfulAsList(cacheSlotAssignments);
    try {
      futureList.get(futuresListTimeoutSecs, TimeUnit.SECONDS);
    } catch (Exception e) {
      futureList.cancel(true);
    }

    int successfulAssignments = successCounter.get();
    int failedAssignments = cacheSlotAssignments.size() - successfulAssignments;

    slotAssignSucceeded.increment(successfulAssignments);
    slotAssignFailed.increment(failedAssignments);

    long assignmentDuration = assignmentTimer.stop(slotAssignTimer);
    LOG.info(
        "Completed cache slot assignment - successfully assigned {} replicas, failed to assign {} replicas in {} ms",
        successfulAssignments,
        failedAssignments,
        TimeUnit.MILLISECONDS.convert(assignmentDuration, TimeUnit.NANOSECONDS));

    return successfulAssignments;
  }
}
