package com.slack.kaldb.clusterManager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.kaldb.util.FutureUtils.successCountingCallback;
import static com.slack.kaldb.util.TimeUtils.nanosToMillis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes snapshots and their associated blob storage objects that both exceed their configured
 * lifespan (plus buffer), and have no associated replicas configured. Snapshot lifespan is expected
 * to be some value greater than the replica lifespan.
 */
@SuppressWarnings("UnstableApiUsage")
public class SnapshotDeletionService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDeletionService.class);

  private static final int THREAD_POOL_SIZE = 1;
  private static final int MAXIMUM_DELETES_PER_SECOND = 10;

  // Additional buffer to wait past expiration before deleting, as a safety buffer
  private static final int DELETE_BUFFER_MINS = 360;

  private final KaldbConfigs.ManagerConfig managerConfig;

  private final ReplicaMetadataStore replicaMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final MeterRegistry meterRegistry;
  private final BlobFs s3BlobFs;

  @VisibleForTesting protected int futuresListTimeoutSecs;

  public static final String SNAPSHOT_DELETE_SUCCESS = "snapshot_delete_success";
  public static final String SNAPSHOT_DELETE_FAILED = "snapshot_delete_failed";
  public static final String SNAPSHOT_DELETE_TIMER = "snapshot_delete_timer";

  private final Counter snapshotDeleteSuccess;
  private final Counter snapshotDeleteFailed;
  private final Timer snapshotDeleteTimer;

  private final ExecutorService executorService =
      Executors.newFixedThreadPool(
          THREAD_POOL_SIZE,
          new ThreadFactoryBuilder()
              .setUncaughtExceptionHandler(
                  (t, e) -> LOG.error("Exception on thread {}: {}", t.getName(), e))
              .setNameFormat("snapshot-deletion-service-%d")
              .build());
  private final RateLimiter rateLimiter = RateLimiter.create(MAXIMUM_DELETES_PER_SECOND);

  public SnapshotDeletionService(
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      BlobFs s3BlobFs,
      KaldbConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {

    checkArgument(
        managerConfig.getSnapshotDeletionServiceConfig().getSnapshotLifespanMins()
            > managerConfig.getReplicaCreationServiceConfig().getReplicaLifespanMins(),
        "SnapshotLifespanMins must be greater than the ReplicaLifespanMins");
    // schedule configs checked as part of the AbstractScheduledService

    this.managerConfig = managerConfig;
    this.replicaMetadataStore = replicaMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.s3BlobFs = s3BlobFs;
    this.meterRegistry = meterRegistry;

    // This functions as the overall "timeout" for deleteExpiredSnapshotsWithoutReplicas, and should
    // not exceed that of the schedule period. This ensures that we never enter a situation where
    // we are queuing faster than we are draining.
    this.futuresListTimeoutSecs =
        managerConfig.getSnapshotDeletionServiceConfig().getSchedulePeriodMins() * 60;

    this.snapshotDeleteSuccess = meterRegistry.counter(SNAPSHOT_DELETE_SUCCESS);
    this.snapshotDeleteFailed = meterRegistry.counter(SNAPSHOT_DELETE_FAILED);
    this.snapshotDeleteTimer = meterRegistry.timer(SNAPSHOT_DELETE_TIMER);
  }

  @Override
  protected void runOneIteration() {
    deleteExpiredSnapshotsWithoutReplicas();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        managerConfig.getScheduleInitialDelayMins(),
        managerConfig.getSnapshotDeletionServiceConfig().getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting snapshot deletion service");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing snapshot deletion service");
    executorService.shutdown();
  }

  /**
   * Deletes snapshots and associated object storage data that have no corresponding replicas.
   * Separate services are responsible for the eviction and deletion of expired replicas, and this
   * service only is expected to remove snapshots that are not being currently served.
   *
   * <p>Consideration should be taken for when configs are changed, such as shortening the replica
   * lifespan. The expectation is that previously created replicas that may have a longer expiration
   * would see that respected throughout the duration of their lifespan.
   *
   * <p>Deletion of expired snapshots should gracefully handle the scenario where immediately prior
   * to reaching the end of the configured snapshot lifespan, an on-demand request is received. In
   * this scenario a new replica would have been created, and the object would be currently
   * downloading for servicing the request. The expectation from the end user would be that the
   * request would still be served, as when the request was made it was still "in compliance" with
   * the lifespan configuration.
   */
  protected int deleteExpiredSnapshotsWithoutReplicas() {
    Timer.Sample deletionTimer = Timer.start(meterRegistry);

    Set<String> snapshotIdsWithReplicas =
        replicaMetadataStore.listSync().stream()
            .map(replicaMetadata -> replicaMetadata.snapshotId)
            .filter(snapshotId -> snapshotId != null && !snapshotId.isEmpty())
            .collect(Collectors.toUnmodifiableSet());

    long expirationCutoff =
        Instant.now()
            .minus(
                managerConfig.getSnapshotDeletionServiceConfig().getSnapshotLifespanMins(),
                ChronoUnit.MINUTES)
            .minus(DELETE_BUFFER_MINS, ChronoUnit.MINUTES)
            .toEpochMilli();
    AtomicInteger successCounter = new AtomicInteger(0);
    List<ListenableFuture<?>> deletedSnapshotList =
        snapshotMetadataStore.listSync().stream()
            // only snapshots that only contain data prior to our cutoff, and have no replicas
            .filter(
                snapshotMetadata ->
                    snapshotMetadata.endTimeEpochMs < expirationCutoff
                        && !snapshotIdsWithReplicas.contains(snapshotMetadata.name))

            // There are cases where we will have LIVE snapshots that might be past the expiration.
            // The primary use case here would be for low traffic clusters. Since they might take
            // a long time to roll over chunks, we may have chunks that are still being actively
            // served from the indexers. To avoid the whole headache of managing all the
            // different states we could be in, we should just disable the deletion of live
            // snapshots whole-cloth. We clean those up when a node boots anyhow
            .filter(snapshotMetadata -> !SnapshotMetadata.isLive(snapshotMetadata))
            .map(
                snapshotMetadata -> {
                  ListenableFuture<?> future =
                      Futures.submit(
                          () -> {
                            try {
                              // These futures are rate-limited so that we can more evenly
                              // distribute
                              // the load to the downstream services (metadata, s3). There is no
                              // urgency to complete the deletes, so limiting the maximum rate
                              // allows
                              // us to avoid unnecessary spikes.
                              rateLimiter.acquire();

                              // First try to delete the object from S3, then delete from metadata
                              // store. If for some reason the object delete fails, it will leave
                              // the
                              // metadata and try again on the next run.
                              URI snapshotUri = URI.create(snapshotMetadata.snapshotPath);
                              LOG.info("Starting delete of snapshot {}", snapshotMetadata);
                              if (s3BlobFs.exists(snapshotUri)) {
                                // Ensure that the file exists before attempting to delete, in case
                                // the previous run successfully deleted the object but failed the
                                // metadata delete. Otherwise, this would be expected to perpetually
                                // fail deleting a non-existing file.
                                if (s3BlobFs.delete(snapshotUri, true)) {
                                  snapshotMetadataStore.deleteSync(snapshotMetadata);
                                } else {
                                  throw new IOException(
                                      String.format(
                                          "Failed to delete '%s' from object store",
                                          snapshotMetadata.snapshotPath));
                                }
                              } else {
                                snapshotMetadataStore.deleteSync(snapshotMetadata);
                              }
                            } catch (Exception e) {
                              LOG.error("Exception deleting snapshot", e);
                              throw e;
                            }
                            return null;
                          },
                          executorService);

                  addCallback(
                      future,
                      successCountingCallback(successCounter),
                      MoreExecutors.directExecutor());
                  return future;
                })
            .collect(Collectors.toUnmodifiableList());

    ListenableFuture<?> futureList = Futures.successfulAsList(deletedSnapshotList);
    try {
      futureList.get(futuresListTimeoutSecs, TimeUnit.SECONDS);
    } catch (Exception e) {
      futureList.cancel(true);
    }
    int successfulDeletions = successCounter.get();

    // failedDeletes = timed out futures
    int failedDeletions = deletedSnapshotList.size() - successfulDeletions;

    snapshotDeleteSuccess.increment(successfulDeletions);
    snapshotDeleteFailed.increment(failedDeletions);

    long deletionDuration = deletionTimer.stop(snapshotDeleteTimer);
    LOG.info(
        "Completed snapshot deletion - successfully deleted {} snapshots, failed to delete {} snapshots in {} ms",
        successfulDeletions,
        failedDeletions,
        nanosToMillis(deletionDuration));

    return successfulDeletions;
  }
}
