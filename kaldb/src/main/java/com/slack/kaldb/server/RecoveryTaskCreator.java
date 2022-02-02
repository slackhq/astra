package com.slack.kaldb.server;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.kaldb.util.FutureUtils.successCountingCallback;
import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for the indexer startup operations like stale live snapshot cleanup.
 * determining the start indexing offset from metadata and optionally creating a recovery task etc.
 */
public class RecoveryTaskCreator {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryTaskCreator.class);
  private static final int SNAPSHOT_OPERATION_TIMEOUT_SECS = 10;

  private final SnapshotMetadataStore snapshotMetadataStore;
  private final RecoveryTaskMetadataStore recoveryTaskMetadataStore;
  private final String partitionId;
  private final long maxOffsetDelay;

  public static final String STALE_SNAPSHOT_DELETE_SUCCESS = "stale_snapshot_delete_success";
  public static final String STALE_SNAPSHOT_DELETE_FAILED = "stale_snapshot_delete_failed";
  public static final String RECOVERY_TASKS_CREATED = "recovery_tasks_created";

  private final Counter snapshotDeleteSuccess;
  private final Counter snapshotDeleteFailed;
  private final Counter recoveryTasksCreated;

  public RecoveryTaskCreator(
      SnapshotMetadataStore snapshotMetadataStore,
      RecoveryTaskMetadataStore recoveryTaskMetadataStore,
      String partitionId,
      long maxOffsetDelay,
      MeterRegistry meterRegistry) {
    checkArgument(
        partitionId != null && !partitionId.isEmpty(), "partitionId shouldn't be null or empty");
    checkArgument(maxOffsetDelay > 0, "maxOffsetDelay should be a positive number");
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.recoveryTaskMetadataStore = recoveryTaskMetadataStore;
    this.partitionId = partitionId;
    this.maxOffsetDelay = maxOffsetDelay;

    snapshotDeleteSuccess = meterRegistry.counter(STALE_SNAPSHOT_DELETE_SUCCESS);
    snapshotDeleteFailed = meterRegistry.counter(STALE_SNAPSHOT_DELETE_FAILED);
    recoveryTasksCreated = meterRegistry.counter(RECOVERY_TASKS_CREATED);
  }

  @VisibleForTesting
  public static List<SnapshotMetadata> getStaleLiveSnapshots(
      List<SnapshotMetadata> snapshots, String partitionId) {
    return snapshots
        .stream()
        .filter(snapshotMetadata -> snapshotMetadata.partitionId.equals(partitionId))
        .filter(SnapshotMetadata::isLive)
        .collect(Collectors.toUnmodifiableList());
  }

  // Get the highest offset for which data is durable for a partition.
  @VisibleForTesting
  public static long getHighestDurableOffsetForPartition(
      List<SnapshotMetadata> snapshots,
      List<RecoveryTaskMetadata> recoveryTasks,
      String partitionId) {

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

  private static String getRecoveryTaskName(long creationTimeEpochMs, String partitionId) {
    return "recoveryTask_" + partitionId + "_" + creationTimeEpochMs;
  }

  @VisibleForTesting
  public List<SnapshotMetadata> deleteStaleLiveSnapshots(List<SnapshotMetadata> snapshots) {
    List<SnapshotMetadata> staleSnapshots = getStaleLiveSnapshots(snapshots, partitionId);
    LOG.info("Deleting {} stale snapshots: {}", staleSnapshots.size(), staleSnapshots);
    int deletedSnapshotCount = deleteSnapshots(snapshotMetadataStore, staleSnapshots);

    int failedDeletes = staleSnapshots.size() - deletedSnapshotCount;
    if (failedDeletes > 0) {
      LOG.warn("Failed to delete {} live snapshots", failedDeletes);
      throw new IllegalStateException("Failed to delete stale live snapshots");
    }

    return staleSnapshots;
  }

  /**
   * To determine the start offset, an indexer performs multiple tasks. First, we clean up all the
   * stale live nodes for this partition so there is only 1 live node per indexer.
   *
   * <p>In Kaldb, the durability of un-indexed data is ensured by Kafka and the durability of
   * indexed data is ensured by S3. So, once the indexer restarts, we need to determine the highest
   * offset that was indexed. To get the latest indexed offset, we get the latest indexed offset
   * from a snapshots for that partition. Since there could also be a recovery task queued up for
   * this partition, we also need to skip the offsets picked up by the recovery task. So, the
   * highest durable offset is the highest offset for a partition among the snapshots and recovery
   * tasks for a partition.
   *
   * <p>The highest durable offset is the start offset for the indexer. If this offset is with in
   * the max start delay of the head, we start indexing. If the current index offset is more than
   * the configured delay, we can't catch up indexing. So, instead of trying to catch up, create a
   * recovery task and start indexing at the current head. This strategy achieves 2 goals: we start
   * indexing fresh data when we are behind, and we add more indexing capacity when needed. The
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
    List<SnapshotMetadata> deletedSnapshots = deleteStaleLiveSnapshots(snapshotsForPartition);

    List<SnapshotMetadata> nonLiveSnapshotsForPartition =
        snapshotsForPartition
            .stream()
            .filter(s -> !deletedSnapshots.contains(s))
            .collect(Collectors.toUnmodifiableList());

    // Get the highest offset that is indexed in durable store.
    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskMetadataStore.listSync();
    long highestDurableOffsetForPartition =
        getHighestDurableOffsetForPartition(
            nonLiveSnapshotsForPartition, recoveryTasks, partitionId);
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
    // for now, so we can investigate.
    if (currentHeadOffsetForPartition < highestDurableOffsetForPartition) {
      final String message =
          String.format(
              "The current head for the partition %d can't "
                  + "be lower than the highest durable offset for that partition %d",
              currentHeadOffsetForPartition, highestDurableOffsetForPartition);
      LOG.error(message);
      throw new IllegalStateException(message);
    }

    // The head offset for Kafka partition is the offset of the next message to be indexed. We
    // assume that offset is passed into this function. The highest durable offset is the partition
    // offset of the message that is indexed. Hence, the offset is incremented by 1 to get the
    // next message.
    long nextOffsetForPartition = highestDurableOffsetForPartition + 1;

    // Create a recovery task if needed.
    if (currentHeadOffsetForPartition - highestDurableOffsetForPartition > maxOffsetDelay) {
      final long creationTimeEpochMs = Instant.now().toEpochMilli();
      final String recoveryTaskName = getRecoveryTaskName(creationTimeEpochMs, partitionId);
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
              nextOffsetForPartition,
              currentHeadOffsetForPartition - 1,
              creationTimeEpochMs));
      LOG.info(
          "Created recovery task {} to catchup. Moving the starting offset to head at {}",
          recoveryTaskName,
          currentHeadOffsetForPartition);
      recoveryTasksCreated.increment();
      return currentHeadOffsetForPartition;
    } else {
      LOG.info(
          "The difference between the last indexed position {} and head location {} is lower "
              + "than max offset {}. So, using {} position as the start offset",
          highestDurableOffsetForPartition,
          currentHeadOffsetForPartition,
          maxOffsetDelay,
          nextOffsetForPartition);
      return nextOffsetForPartition;
    }
  }

  private int deleteSnapshots(
      SnapshotMetadataStore snapshotMetadataStore, List<SnapshotMetadata> snapshotsToBeDeleted) {
    LOG.info("Deleting {} snapshots: {}", snapshotsToBeDeleted.size(), snapshotsToBeDeleted);

    AtomicInteger successCounter = new AtomicInteger(0);
    List<? extends ListenableFuture<?>> deletionFutures =
        snapshotsToBeDeleted
            .stream()
            .map(
                snapshot -> {
                  ListenableFuture<?> future = snapshotMetadataStore.delete(snapshot);
                  addCallback(
                      future,
                      successCountingCallback(successCounter),
                      MoreExecutors.directExecutor());
                  return future;
                })
            .collect(Collectors.toUnmodifiableList());

    //noinspection UnstableApiUsage
    ListenableFuture<?> futureList = Futures.successfulAsList(deletionFutures);
    try {
      futureList.get(SNAPSHOT_OPERATION_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (Exception e) {
      futureList.cancel(true);
    }

    final int successfulDeletions = successCounter.get();
    int failedDeletions = snapshotsToBeDeleted.size() - successfulDeletions;

    snapshotDeleteSuccess.increment(successfulDeletions);
    snapshotDeleteFailed.increment(failedDeletions);

    if (successfulDeletions == snapshotsToBeDeleted.size()) {
      LOG.info("Successfully deleted all {} snapshots.", successfulDeletions);
    } else {
      LOG.warn(
          "Failed to delete {} snapshots within {} secs.",
          SNAPSHOT_OPERATION_TIMEOUT_SECS,
          snapshotsToBeDeleted.size() - successfulDeletions);
    }
    return successfulDeletions;
  }
}
