package com.slack.kaldb.server;

import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.util.SnapshotsUtil;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for the indexer startup operations like stale live snapshot cleanup.
 * determining the start indexing offset and create Recovery task etc..
 */
public class RecoveryTaskFactory {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryTaskFactory.class);

  private final SnapshotMetadataStore snapshotMetadataStore;
  private final RecoveryTaskMetadataStore recoveryTaskMetadataStore;
  private final String partitionId;

  public RecoveryTaskFactory(
      SnapshotMetadataStore snapshotMetadataStore,
      RecoveryTaskMetadataStore recoveryTaskMetadataStore,
      String partitionId) {
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.recoveryTaskMetadataStore = recoveryTaskMetadataStore;
    this.partitionId = partitionId;
  }

  public List<SnapshotMetadata> getStaleLiveSnapshots(List<SnapshotMetadata> snapshots) {
    List<SnapshotMetadata> snapshotsForPartition =
        snapshots
            .stream()
            .filter(snapshotMetadata -> snapshotMetadata.partitionId.equals(partitionId))
            .collect(Collectors.toUnmodifiableList());
    return snapshotsForPartition
        .stream()
        .filter(SnapshotMetadata::isLive)
        .collect(Collectors.toUnmodifiableList());
  }

  public int deleteStaleLiveSnapsnots(List<SnapshotMetadata> snapshots) {
    List<SnapshotMetadata> staleSnapshots = getStaleLiveSnapshots(snapshots);
    LOG.info("Deleting {} stale snapshots: {}", staleSnapshots.size(), staleSnapshots);
    return SnapshotsUtil.deleteSnapshots(snapshotMetadataStore, staleSnapshots);
  }

  /*
  TODO: Implement start up operation.
  TODO: Implement recovery task creation.
  start up tasks:
      * Cleanup stale live nodes.
      * Create a recovery task if needed.
      * Get max offset to start indexing.
   */
  // Get the highest offset for which data is durable for a partition.
  public long getHigestDurableOffsetForPartition(
      List<SnapshotMetadata> snapshots, List<RecoveryTaskMetadata> recoveryTasks) {

    long maxSnapshotOffset =
        snapshots
            .stream()
            .filter(snapshot -> snapshot.partitionId.equals(partitionId))
            .mapToLong(snapshot -> snapshot.maxOffset)
            .max()
            .orElse(-1);

    long maxRecoveryOffset =
        recoveryTasks
            .stream()
            .filter(recoveryTaskMetadata -> recoveryTaskMetadata.partitionId.equals(partitionId))
            .mapToLong(recoveryTaskMetadata -> recoveryTaskMetadata.endOffset)
            .max()
            .orElse(-1);

    return Math.max(maxRecoveryOffset, maxSnapshotOffset);
  }
}
