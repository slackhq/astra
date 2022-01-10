package com.slack.kaldb.clusterManager;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.kaldb.config.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.util.FutureUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes expired and unassigned replicas. If a replica is still assigned, marked for eviction, or
 * in the process of evicting this will not delete the replica. Any replicas that are not deleted in
 * a given run will be attempted in the following execution.
 */
public class ReplicaDeletionService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaDeletionService.class);
  private final KaldbConfigs.ManagerConfig managerConfig;

  private final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final ReplicaMetadataStore replicaMetadataStore;
  private final MeterRegistry meterRegistry;

  @VisibleForTesting protected int futuresListTimeoutSecs = DEFAULT_ZK_TIMEOUT_SECS;

  public static final String REPLICA_DELETE_SUCCESS = "replica_delete_success";
  public static final String REPLICA_DELETE_FAILED = "replica_delete_failed";
  public static final String REPLICA_DELETE_TIMER = "replica_delete_timer";

  private final Counter replicaDeleteSuccess;
  private final Counter replicaDeleteFailed;
  private final Timer replicaDeleteTimer;

  public ReplicaDeletionService(
      CacheSlotMetadataStore cacheSlotMetadataStore,
      ReplicaMetadataStore replicaMetadataStore,
      KaldbConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {
    this.cacheSlotMetadataStore = cacheSlotMetadataStore;
    this.replicaMetadataStore = replicaMetadataStore;
    this.managerConfig = managerConfig;
    this.meterRegistry = meterRegistry;

    // schedule configs checked as part of the AbstractScheduledService

    replicaDeleteSuccess = meterRegistry.counter(REPLICA_DELETE_SUCCESS);
    replicaDeleteFailed = meterRegistry.counter(REPLICA_DELETE_FAILED);
    replicaDeleteTimer = meterRegistry.timer(REPLICA_DELETE_TIMER);
  }

  @Override
  protected void runOneIteration() {
    deleteExpiredUnassignedReplicas();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        managerConfig.getScheduleInitialDelayMins(),
        managerConfig.getReplicaDeletionServiceConfig().getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting replica deletion service");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closed replica deletion service");
  }

  /**
   * Deletes replicas that are expired and not currently assigned to a given cache slot. Return
   * value is the amount of successful deletion requests.
   */
  protected int deleteExpiredUnassignedReplicas() {
    Timer.Sample deleteTimer = Timer.start(meterRegistry);

    Set<String> replicaIdsWithAssignments =
        cacheSlotMetadataStore
            .getCached()
            .stream()
            .map(cacheSlotMetadata -> cacheSlotMetadata.replicaId)
            .collect(Collectors.toUnmodifiableSet());

    AtomicInteger successCounter = new AtomicInteger(0);
    long milliNow = Instant.now().toEpochMilli();
    List<ListenableFuture<?>> replicaDeletions =
        replicaMetadataStore
            .getCached()
            .stream()
            .filter(
                replicaMetadata ->
                    replicaMetadata.expireAfterUtc < milliNow
                        && !replicaIdsWithAssignments.contains(replicaMetadata.name))
            .map(
                (replicaMetadata) -> {
                  ListenableFuture<?> future = replicaMetadataStore.delete(replicaMetadata);
                  addCallback(
                      future,
                      FutureUtils.successCountingCallback(successCounter),
                      MoreExecutors.directExecutor());
                  return future;
                })
            .collect(Collectors.toUnmodifiableList());

    ListenableFuture<?> futureList = Futures.successfulAsList(replicaDeletions);
    try {
      futureList.get(futuresListTimeoutSecs, TimeUnit.SECONDS);
    } catch (Exception e) {
      futureList.cancel(true);
    }

    int successfulDeletions = successCounter.get();
    int failedDeletions = replicaDeletions.size() - successfulDeletions;

    replicaDeleteSuccess.increment(successfulDeletions);
    replicaDeleteFailed.increment(failedDeletions);

    long deleteDuration = deleteTimer.stop(replicaDeleteTimer);
    LOG.info(
        "Completed replica deletions - successfully deleted {} replicas, failed to delete {} replicas in {} ms",
        successfulDeletions,
        failedDeletions,
        TimeUnit.MILLISECONDS.convert(deleteDuration, TimeUnit.NANOSECONDS));

    return successfulDeletions;
  }
}
