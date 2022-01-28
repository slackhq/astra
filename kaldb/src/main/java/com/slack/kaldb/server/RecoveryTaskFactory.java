package com.slack.kaldb.server;

import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
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
    checkState(
        partitionId != null && !partitionId.isEmpty(), "partitionId shouldn't be null or empty");
    checkState(maxOffsetDelay > 0, "maxOffsetDelay should be a positive number");
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.recoveryTaskMetadataStore = recoveryTaskMetadataStore;
    this.partitionId = partitionId;
    this.maxOffsetDelay = maxOffsetDelay;
  }

  @VisibleForTesting
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

  @VisibleForTesting
  public List<SnapshotMetadata> deleteStaleLiveSnapsnots(List<SnapshotMetadata> snapshots) {
    List<SnapshotMetadata> staleSnapshots = getStaleLiveSnapshots(snapshots);
    LOG.info("Deleting {} stale snapshots: {}", staleSnapshots.size(), staleSnapshots);
    int deletedSnapshotCount = SnapshotsUtil.deleteSnapshots(snapshotMetadataStore, staleSnapshots);

    int failedDeletes = staleSnapshots.size() - deletedSnapshotCount;
    if (failedDeletes > 0) {
      LOG.warn("Failed to delete {} live snapshots", failedDeletes);
      throw new IllegalStateException("Failed to delete stale live snapshots");
    }

    return staleSnapshots;
  }

  // Get the highest offset for which data is durable for a partition.
  @VisibleForTesting
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
   * To determine the start offset, an indexer performs multiple tasks. First, we clean up all the
   * stale live nodes for this partition so there is only 1 live node per indexer.
   *
   * <p>In Kaldb, the durability of unindexed data is ensured by Kafka and the durability of indexed
   * data is ensured by S3. So, once the indexer restarts, we need to determine the highest offset
   * that was indexed. To get the latest indexed offset, we get the latest indexed offset from a
   * snapshots for that partition. Since there could also be a recovery task queued up for this
   * partition, we also need to skip the offsets picked up by the recovery task. So, the highest
   * durable offset is the highest offset for a partition among the snapshots and recovery tasks for
   * a partition.
   *
   * <p>The highest durable offset is the start offset for the indexer. If this offset is with in
   * the max start delay of the head, we start indexing. If the current index offset is more than
   * the configured delay, we can't catch up indexing. So, instead of trying to catch up, create a
   * recovery task and start indexing at the current head. This strategy achieves 2 goals: we start
   * indexing fresh data when we are behind and we add more indexing capacity when needed. The
   * recovery task offsets are [startOffset, endOffset]. If a recovery task is created, we start
   * indexing at the offset after the recovery task.
   *
   * <p>When there is no offset data for a partition, return -1. In that case, the consumer would
   * have to start indexing the data from the earliest offset.
   */
  public long determineStartingOffset(long currentHeadOffsetForPartition) {
    // Filter stale snapshots for partition.
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    List<SnapshotMetadata> snapshotsForPartition =
        snapshots
            .stream()
            .filter(snapshotMetadata -> snapshotMetadata.partitionId.equals(partitionId))
            .collect(Collectors.toUnmodifiableList());
    List<SnapshotMetadata> deletedSnapshots = deleteStaleLiveSnapsnots(snapshotsForPartition);

    List<SnapshotMetadata> nonLiveSnapshotsForPartition =
        snapshotsForPartition
            .stream()
            .filter(s -> !deletedSnapshots.contains(s))
            .collect(Collectors.toUnmodifiableList());

    // Get the highest offset that is indexed in durable store.
    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskMetadataStore.listSync();
    long highestDurableOffsetForPartition =
        getHigestDurableOffsetForPartition(nonLiveSnapshotsForPartition, recoveryTasks);
    LOG.info(
        "The highest durable offset for partition {} is {}",
        partitionId,
        highestDurableOffsetForPartition);

    if (highestDurableOffsetForPartition <= 0) {
      LOG.info("There is no prior offset for this partition {}.", partitionId);
      return highestDurableOffsetForPartition;
    }

    // The current head offset shouldn't be lower than the highest durable offset. If it is it
    // means that we indexed more data than the current head offset. This is either a bug in the
    // offset handling mechanism or the kafka partition has rolled over. We throw an exception
    // for now so we can investigate.
    if (currentHeadOffsetForPartition < highestDurableOffsetForPartition) {
      throw new IllegalStateException(
          String.format(
              "The current head for the partition %d can't "
                  + "be lower than the highest durable offset for that partition %d",
              currentHeadOffsetForPartition, highestDurableOffsetForPartition));
    }

    // Create a recovery task if needed.
    if (currentHeadOffsetForPartition - highestDurableOffsetForPartition > maxOffsetDelay) {
      final long creationTimeEpochMs = Instant.now().toEpochMilli();
      final String recoveryTaskName = "recoveryTask_" + partitionId + "_" + creationTimeEpochMs;
      LOG.info(
          "Recovery task needed. The current position {} and head location {} are higher than max"
              + " offset {}",
          highestDurableOffsetForPartition,
          currentHeadOffsetForPartition,
          maxOffsetDelay);
      recoveryTaskMetadataStore.createSync(
          new RecoveryTaskMetadata(
              recoveryTaskName,
              partitionId,
              highestDurableOffsetForPartition + 1,
              currentHeadOffsetForPartition - 1,
              creationTimeEpochMs));
      LOG.info(
          "Created recovery task {} to catchup. Moving the starting offset to head at {}",
          recoveryTaskName,
          currentHeadOffsetForPartition);
      return currentHeadOffsetForPartition;
    } else {
      long newOffset = highestDurableOffsetForPartition + 1;
      LOG.info(
          "The difference between the last indexed position {} and head location {} is lower "
              + "than max offset {}. So, using {} position as the start offset",
          highestDurableOffsetForPartition,
          currentHeadOffsetForPartition,
          maxOffsetDelay,
          newOffset);
      return newOffset;
    }
  }
}
