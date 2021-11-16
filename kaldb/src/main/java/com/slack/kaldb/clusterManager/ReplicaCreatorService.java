package com.slack.kaldb.clusterManager;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the lifecycle for the Replica metadata type. At least one Replica is expected to be
 * created once a snapshot has been published by an indexer node.
 *
 * <p>Each Replica is then expected to be assigned to a Cache node, depending on availability, by
 * the cache assignment service in the cluster manager
 */
public class ReplicaCreatorService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaCreatorService.class);
  private final KaldbConfigs.ManagerConfig.ReplicaServiceConfig replicaServiceConfig;

  private final ReplicaMetadataStore replicaMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final MeterRegistry meterRegistry;

  public static final String REPLICAS_CREATED = "replicas_created";
  public static final String REPLICAS_FAILED = "replicas_failed";
  public static final String REPLICA_ASSIGNMENT_TIMER = "replica_assignment_timer";

  private final Counter replicasCreated;
  private final Counter replicasFailed;
  private final Timer replicaAssignmentTimer;

  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private ScheduledFuture<?> pendingTask;

  public ReplicaCreatorService(
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      KaldbConfigs.ManagerConfig.ReplicaServiceConfig replicaServiceConfig,
      MeterRegistry meterRegistry) {

    this.replicaMetadataStore = replicaMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.replicaServiceConfig = replicaServiceConfig;

    this.meterRegistry = meterRegistry;
    this.replicasCreated = meterRegistry.counter(REPLICAS_CREATED);
    this.replicasFailed = meterRegistry.counter(REPLICAS_FAILED);
    this.replicaAssignmentTimer = meterRegistry.timer(REPLICA_ASSIGNMENT_TIMER);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting replica creator service");
    snapshotMetadataStore.addListener(this::runOneIteration);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing replica create service");

    executorService.shutdownNow();

    replicaMetadataStore.close();
    snapshotMetadataStore.close();

    LOG.info("Closed replica create service");
  }

  @Override
  protected void runOneIteration() {
    // we want to execute at most once task every N seconds, regardless of how many events happen
    // if there is no pending task, or the pending task has already started make a new one
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executorService.schedule(
              this::createReplicasForUnassignedSnapshots,
              replicaServiceConfig.getEventAggregationSecs(),
              TimeUnit.SECONDS);
    } else {
      LOG.debug(
          "Replica task already queued for execution, will run in {} ms",
          pendingTask.getDelay(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected Scheduler scheduler() {
    // run one iteration at startup, and then every 15 mins after that
    return Scheduler.newFixedRateSchedule(
        replicaServiceConfig.getScheduleInitialDelayMins(),
        replicaServiceConfig.getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  private void createReplicasForUnassignedSnapshots() {
    LOG.info("Starting replica creation for unassigned snapshots");
    Timer.Sample assignmentTimer = Timer.start(meterRegistry);

    // calculate a set of all snapshot IDs that have replicas already created
    // we use a HashSet so that the contains() is a constant time operation
    HashSet<String> snapshotsWithReplicas =
        replicaMetadataStore
            .getCached()
            .stream()
            .map(replicaMetadata -> replicaMetadata.snapshotId)
            .collect(Collectors.toCollection(HashSet::new));

    snapshotMetadataStore
        .getCached()
        .stream()
        .filter((snapshotMetadata) -> !snapshotsWithReplicas.contains(snapshotMetadata.snapshotId))
        .forEach(
            unassignedSnapshot -> {
              for (int i = 0; i < replicaServiceConfig.getReplicasPerSnapshot(); i++) {
                try {
                  replicaMetadataStore.createSync(
                      replicaMetadataFromSnapshotId(unassignedSnapshot.snapshotId));
                  replicasCreated.increment();
                } catch (Exception e) {
                  LOG.error(
                      "Error creating replica for snapshot {}", unassignedSnapshot.snapshotId);
                  replicasFailed.increment();
                }
              }
            });

    assignmentTimer.stop(replicaAssignmentTimer);
    LOG.info("Completed replica creation for unassigned snapshots");
  }

  public static ReplicaMetadata replicaMetadataFromSnapshotId(String snapshotId) {
    return new ReplicaMetadata(UUID.randomUUID().toString(), snapshotId);
  }
}
