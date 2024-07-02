package com.slack.astra.clusterManager;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.astra.server.AstraConfig.DEFAULT_ZK_TIMEOUT_SECS;
import static com.slack.astra.util.TimeUtils.nanosToMillis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.util.FutureUtils;
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
  private final AstraConfigs.ManagerConfig managerConfig;

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
  private CacheNodeAssignmentStore cacheNodeAssignmentStore;

  public ReplicaDeletionService(
      CacheSlotMetadataStore cacheSlotMetadataStore,
      ReplicaMetadataStore replicaMetadataStore,
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      AstraConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {
    this.cacheSlotMetadataStore = cacheSlotMetadataStore;
    this.replicaMetadataStore = replicaMetadataStore;
    this.managerConfig = managerConfig;
    this.meterRegistry = meterRegistry;
    this.cacheNodeAssignmentStore = cacheNodeAssignmentStore;

    // schedule configs checked as part of the AbstractScheduledService

    replicaDeleteSuccess = meterRegistry.counter(REPLICA_DELETE_SUCCESS);
    replicaDeleteFailed = meterRegistry.counter(REPLICA_DELETE_FAILED);
    replicaDeleteTimer = meterRegistry.timer(REPLICA_DELETE_TIMER);
  }

  @Override
  protected void runOneIteration() {
    deleteExpiredUnassignedReplicas(Instant.now());
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
   *
   * @param deleteOlderThan Will only delete replicas that have an expiration prior to this value
   */
  protected int deleteExpiredUnassignedReplicas(Instant deleteOlderThan) {
    Timer.Sample deleteTimer = Timer.start(meterRegistry);

    Set<String> replicaIdsWithAssignments =
        cacheSlotMetadataStore.listSync().stream()
            .map(cacheSlotMetadata -> cacheSlotMetadata.replicaId)
            .collect(Collectors.toSet());

    replicaIdsWithAssignments.addAll(
        cacheNodeAssignmentStore.listSync().stream()
            .map(assignment -> assignment.replicaId)
            .collect(Collectors.toUnmodifiableSet()));

    AtomicInteger successCounter = new AtomicInteger(0);
    List<ListenableFuture<?>> replicaDeletions =
        replicaMetadataStore.listSync().stream()
            .filter(
                replicaMetadata ->
                    replicaMetadata.expireAfterEpochMs < deleteOlderThan.toEpochMilli()
                        && !replicaIdsWithAssignments.contains(replicaMetadata.name))
            .map(
                (replicaMetadata) -> {
                  // todo - consider refactoring this to return a completable future instead
                  ListenableFuture<?> future =
                      JdkFutureAdapters.listenInPoolThread(
                          replicaMetadataStore.deleteAsync(replicaMetadata).toCompletableFuture());
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
        nanosToMillis(deleteDuration));

    return successfulDeletions;
  }
}
