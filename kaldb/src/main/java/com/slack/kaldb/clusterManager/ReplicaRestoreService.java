package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.clusterManager.ReplicaCreationService.replicaMetadataFromSnapshotId;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.naming.SizeLimitExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduled service responsible for restoring user requested Snapshots that have expired. Users can
 * call the queueSnapshotsForRestoration to queue Snapshots for restoration in the future. This
 * service will automatically handle de-duping, ensuring that no Snapshot is restored more than
 * once. Additionally, the maximum number of Snapshots that can be requested at once is also
 * configurable to prevent overwhelming the service.
 */
public class ReplicaRestoreService extends AbstractScheduledService {
  private ScheduledFuture<?> pendingTask;
  private final KaldbConfigs.ManagerConfig managerConfig;
  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setNameFormat("replica-restore-service-%d").build());
  private final BlockingQueue<SnapshotMetadata> queue = new LinkedBlockingQueue<>();
  private final ReplicaMetadataStore replicaMetadataStore;
  private final MeterRegistry meterRegistry;

  protected static final Logger LOG = LoggerFactory.getLogger(ReplicaCreationService.class);
  public static String REPLICAS_CREATED = "replicas_created";
  public static String REPLICAS_FAILED = "replicas_failed";
  public static String REPLICAS_SKIPPED = "replicas_skipped";
  public static String REPLICAS_RESTORE_TIMER = "replicas_restore_timer";

  private final Counter.Builder replicasCreated;
  private final Counter.Builder replicasFailed;
  private final Counter.Builder replicasSkipped;
  private final Timer.Builder replicasRestoreTimer;

  public ReplicaRestoreService(
      ReplicaMetadataStore replicaMetadataStore,
      MeterRegistry meterRegistry,
      KaldbConfigs.ManagerConfig managerConfig) {
    this.managerConfig = managerConfig;
    this.replicaMetadataStore = replicaMetadataStore;
    this.meterRegistry = meterRegistry;

    this.replicasCreated = Counter.builder(REPLICAS_CREATED);
    this.replicasFailed = Counter.builder(REPLICAS_FAILED);
    this.replicasSkipped = Counter.builder(REPLICAS_SKIPPED);
    this.replicasRestoreTimer = Timer.builder(REPLICAS_RESTORE_TIMER);
  }

  @Override
  protected void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executorService.schedule(
              this::restoreQueuedSnapshots,
              managerConfig.getEventAggregationSecs(),
              TimeUnit.SECONDS);
    } else {
      LOG.info(
          "Replica restore task already scheduled, will run in {} ms",
          pendingTask.getDelay(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting replica restore service");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing replica restore service");
    executorService.shutdownNow();
    LOG.info("Closed replica restore service");
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        managerConfig.getScheduleInitialDelayMins(),
        managerConfig.getReplicaRestoreServiceConfig().getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  /**
   * Queues Snapshots to have replicas created for them in the future. If the number of Snapshots
   * exceeds the maximum limit per request, a SizeLimitExceededException will be thrown.
   *
   * @throws SizeLimitExceededException Thrown when the number of Snapshots queued in one call
   *     exceeds maxReplicasPerRequest
   * @param snapshotsToRestore List of Snapshots to be queued for restoration
   */
  public synchronized void queueSnapshotsForRestoration(List<SnapshotMetadata> snapshotsToRestore)
      throws SizeLimitExceededException {
    if (snapshotsToRestore.size()
        >= managerConfig.getReplicaRestoreServiceConfig().getMaxReplicasPerRequest()) {
      throw new SizeLimitExceededException(
          "Number of replicas requested exceeds maxReplicasPerRequest limit");
    }
    queue.addAll(snapshotsToRestore);
    LOG.info("Current size of Snapshot restoration queue: {} ", queue.size());
    runOneIteration();
  }

  /** Drains the current queue and creates replicas as required. Called by scheduler. */
  private void restoreQueuedSnapshots() {
    if (queue.isEmpty()) {
      return;
    }

    // foreach partition
    for (String replicaPartition :
        managerConfig.getReplicaRestoreServiceConfig().getReplicaPartitionsList()) {
      Timer.Sample restoreReplicasTimer = Timer.start(meterRegistry);

      List<SnapshotMetadata> snapshotsToRestore = new ArrayList<>();
      Set<String> createdReplicas = new HashSet<>();

      for (ReplicaMetadata replicaMetadata : replicaMetadataStore.listSync()) {
        createdReplicas.add(replicaMetadata.snapshotId);
      }

      queue.drainTo(snapshotsToRestore);

      for (SnapshotMetadata snapshotMetadata : snapshotsToRestore) {
        try {
          restoreOrSkipSnapshot(snapshotMetadata, replicaPartition, createdReplicas);
          createdReplicas.add(snapshotMetadata.snapshotId);
        } catch (InterruptedException e) {
          LOG.error(
              "Something went wrong dequeueing snapshot ID {} for partition {}",
              snapshotMetadata.snapshotId,
              replicaPartition,
              e);
          replicasFailed
              .tag("replicaPartition", replicaPartition)
              .register(meterRegistry)
              .increment();
        }
      }
      restoreReplicasTimer.stop(
          replicasRestoreTimer.tag("replicaPartition", replicaPartition).register(meterRegistry));
      LOG.info(
          "Restored {} snapshots for partition {}.", snapshotsToRestore.size(), replicaPartition);
    }
  }

  /** Creates replica from given snapshot if its ID doesn't already exist in createdReplicas */
  private void restoreOrSkipSnapshot(
      SnapshotMetadata snapshot, String replicaPartition, Set<String> createdReplicas)
      throws InterruptedException {
    if (!createdReplicas.contains(snapshot.snapshotId)) {
      LOG.info("Restoring replica with ID {}", snapshot.snapshotId);

      try {
        replicaMetadataStore.createSync(
            replicaMetadataFromSnapshotId(
                snapshot.snapshotId,
                replicaPartition,
                Instant.now()
                    .plus(
                        managerConfig.getReplicaRestoreServiceConfig().getReplicaLifespanMins(),
                        ChronoUnit.MINUTES),
                true));
      } catch (Exception e) {
        LOG.error("Error restoring replica for snapshot {}", snapshot.snapshotId, e);
      }
      createdReplicas.add(snapshot.snapshotId);
      replicasCreated.tag("replicaPartition", replicaPartition).register(meterRegistry).increment();
    } else {
      LOG.info("Skipping Snapshot ID {} ", snapshot.snapshotId);
      replicasSkipped.tag("replicaPartition", replicaPartition).register(meterRegistry).increment();
    }
  }
}
