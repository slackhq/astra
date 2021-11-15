package com.slack.kaldb.clusterManager;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.HashSet;
import java.util.UUID;
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
public class ReplicaCreatorService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaCreatorService.class);

  private final int replicasPerSnapshot;

  private final ReplicaMetadataStore replicaMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final MeterRegistry meterRegistry;

  public static final String REPLICAS_CREATED = "replicas_created";
  public static final String REPLICAS_FAILED = "replicas_failed";
  public static final String REPLICA_ASSIGNMENT_TIMER = "replica_assignment_timer";

  private final Counter replicasCreated;
  private final Counter replicasFailed;
  private final Timer replicaAssignmentTimer;

  public ReplicaCreatorService(
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      int replicasPerSnapshot,
      MeterRegistry meterRegistry) {

    this.replicaMetadataStore = replicaMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.replicasPerSnapshot = replicasPerSnapshot;

    this.meterRegistry = meterRegistry;
    this.replicasCreated = meterRegistry.counter(REPLICAS_CREATED);
    this.replicasFailed = meterRegistry.counter(REPLICAS_FAILED);
    this.replicaAssignmentTimer = meterRegistry.timer(REPLICA_ASSIGNMENT_TIMER);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting replica creator service");
    snapshotMetadataStore.addListener(this::createReplicasForUnassignedSnapshots);

    // we need to fire this at startup, so any snapshots that were created while we were offline are
    // handled
    createReplicasForUnassignedSnapshots();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing replica create service");

    replicaMetadataStore.close();
    snapshotMetadataStore.close();

    LOG.info("Closed replica create service");
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
              for (int i = 0; i < replicasPerSnapshot; i++) {
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
