package com.slack.kaldb.clusterManager;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
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

  public static final String REPLICAS_CREATED = "replicas_created";
  private final Counter replicasCreated;

  public ReplicaCreatorService(
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      int replicasPerSnapshot,
      MeterRegistry meterRegistry) {

    this.replicaMetadataStore = replicaMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.replicasPerSnapshot = replicasPerSnapshot;
    this.replicasCreated = meterRegistry.counter(REPLICAS_CREATED);
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
    snapshotMetadataStore
        .getCached()
        .stream()
        .filter(
            snapshotMetadata ->
                replicaMetadataStore
                    .getCached()
                    .stream()
                    .noneMatch(
                        replicaMetadata ->
                            replicaMetadata.snapshotId.equals(snapshotMetadata.snapshotId)))
        .forEach(
            unassignedSnapshot -> {
              for (int i = 0; i < replicasPerSnapshot; i++) {
                replicaMetadataStore.createSync(
                    new ReplicaMetadata(
                        UUID.randomUUID().toString(), unassignedSnapshot.snapshotId));
                replicasCreated.increment();
              }
            });

    LOG.info("Completed replica creation for unassigned snapshots");
  }
}
