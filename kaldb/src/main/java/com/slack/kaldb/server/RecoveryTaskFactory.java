package com.slack.kaldb.server;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.clusterManager.RecoveryTaskAssignmentService;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.util.CountingFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.curator.shaded.com.google.common.util.concurrent.Futures.addCallback;

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

  private List<SnapshotMetadata> getStaleLiveSnapshots(List<SnapshotMetadata> snapshots) {
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

  public void deleteStaleSnapshots(List<SnapshotMetadata> staleSnapshots) {
    AtomicInteger successCounter = new AtomicInteger(0);

    // We only expect 1 stale chunk at a time, so a sync delete is fine.
    staleSnapshots
            .stream()
            .map( snapshot -> {
              ListenableFuture<?> future = snapshotMetadataStore.delete(snapshot);
              addCallback(future, new CountingFuture(), MoreExecutors.directExecutor());
            }).collect(Collectors.toUnmodifiableList());
  }

  public long getStartOffsetForPartition(
      List<SnapshotMetadata> snapshots,
      List<RecoveryTaskMetadata> recoveryTasks) {

    Long maxSnapshotOffset =
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
