package com.slack.kaldb.server;

import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.util.SnapshotsUtil;
import java.time.Instant;
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
  private final long maxOffsetDelay;

  public RecoveryTaskFactory(
      SnapshotMetadataStore snapshotMetadataStore,
      RecoveryTaskMetadataStore recoveryTaskMetadataStore,
      String partitionId,
      long maxOffsetDelay) {
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.recoveryTaskMetadataStore = recoveryTaskMetadataStore;
    this.partitionId = partitionId;
    this.maxOffsetDelay = maxOffsetDelay;
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

  /**
   * If the current index offset is more than the configured delay, we can't catch up indexing. So,
   * instead of trying to catch up, create a recovery task and start indexing at the current head.
   * This strategy achieves 2 goals: we start indexing fresh data when we are behind and we add more
   * indexing capacity when needed.
   */
  public long getStartOffset(long currentOffset, long currentHeadOffset) {
    if (currentHeadOffset - currentOffset > maxOffsetDelay) {
      // TODO: Name task better.
      // TODO: created time is in ms?
      // TODO: are offsets inclusive? current head offset -1?
      // TODO: Make sure the offsets are inclusive
      recoveryTaskMetadataStore.createSync(
          new RecoveryTaskMetadata(
              "recoveryTask" + partitionId,
              partitionId,
              currentOffset,
              currentHeadOffset,
              Instant.now().toEpochMilli()));
      return currentHeadOffset;
    } else {
      return currentOffset;
    }
  }
}
