package com.slack.kaldb.server;

import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class is responsible for the indexer startup operations like stale live snapshot cleanup.
 * determining the start indexing offset and create Recovery task etc..
 */
public class RecoveryTaskFactory {
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final RecoveryTaskMetadataStore recoveryTaskMetadataStore;

  public RecoveryTaskFactory(
      SnapshotMetadataStore snapshotMetadataStore,
      RecoveryTaskMetadataStore recoveryTaskMetadataStore) {
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.recoveryTaskMetadataStore = recoveryTaskMetadataStore;
  }

  public void performStartupOperations(String partitionId) {

    // Stale live snapshot cleanup.
    deleteStaleSnapshots(partitionId);
    // Determining the start indexing offset.
    // Create Recovery task.
  }

  private List<SnapshotMetadata> getStaleLiveSnapshots(String partitionId) {
    // TODO: list sync is an expensive operation. Try to do it only once per indexer start up.
    List<SnapshotMetadata> snapshotsForPartition =
        snapshotMetadataStore
            .listSync()
            .stream()
            .filter(snapshotMetadata -> snapshotMetadata.partitionId.equals(partitionId))
            .collect(Collectors.toUnmodifiableList());
    return snapshotsForPartition
        .stream()
        .filter(snapshotMetadata -> SnapshotMetadata.isLive(snapshotMetadata))
        .collect(Collectors.toUnmodifiableList());
  }

  public void deleteStaleSnapshots(String partitionId) {
    List<SnapshotMetadata> staleSnapshots = getStaleLiveSnapshots(partitionId);
    // We only expect 1 stale chunk at a time, so a sync delete is fine.
    staleSnapshots
        .stream()
        .forEach(snapshotMetadata -> snapshotMetadataStore.deleteSync(snapshotMetadata));
  }

  public int getStartOffsetForPartition(String partitionId) {
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    Optional<Long> maxSnapshotOffset = snapshots.stream()
            .filter(snapshot -> snapshot.partitionId.equals(partitionId))
            .collect(Collectors.maxBy((SnapshotMetadata x, SnapshotMetadata y) -> x.maxOffset - y.maxOffset));

    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskMetadataStore.listSync();
    Optional<Long> maxRecoveryOffset = recoveryTasks.stream()
            .filter(recoveryTaskMetadata -> recoveryTaskMetadata.partitionId.equals(recoveryTaskMetadata))
            .collect(Collectors.maxBy((x, y) -> x.endOffset - y.endOffset));

    return Math.max(maxRecoveryOffset.orElse(0), maxRecoveryOffset.orElse(0));
  }
}
