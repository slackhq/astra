package com.slack.kaldb.chunk;

import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import com.slack.kaldb.logstore.search.LogIndexSearcherImpl;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;

/**
 * ChunkInfo class holds the metadata about a single Chunk. This metdata is used by components like
 * ChunkManager to manage the data in a chunk and to query it during search. We only expect one
 * ChunkInfo object per chunk.
 *
 * <p>NOTE: The metadata in this class consists of 2 parts: (a) metadata related to indexing and (b)
 * metadata related to searching once the chunk is immutable. In future, consider separating this
 * code into multiple, classes.
 *
 * <p>TODO: Have a read only chunk info for read only chunks so we don't accidentally update it.
 * TODO: Add a state machine for a chunk?
 */
public class ChunkInfo {
  public static final long MAX_FUTURE_TIME = Instant.ofEpochSecond(253402329599L).toEpochMilli();
  public static final int DEFAULT_MAX_OFFSET = 0;
  private static final String DEFAULT_KAFKA_PARTITION_ID = "";

  // todo - remove data directory argument once all the data is in the snapshot
  public static ChunkInfo fromSnapshotMetadata(
      SnapshotMetadata snapshotMetadata, Path dataDirectory) {
    ChunkInfo chunkInfo =
        new ChunkInfo(
            snapshotMetadata.snapshotId,
            Instant.now().toEpochMilli(),
            snapshotMetadata.endTimeUtc,
            snapshotMetadata.startTimeUtc,
            snapshotMetadata.endTimeUtc,
            snapshotMetadata.endTimeUtc,
            -1,
            -1,
            DEFAULT_MAX_OFFSET,
            DEFAULT_KAFKA_PARTITION_ID);

    try {
      chunkInfo.setChunkSize(Files.size(dataDirectory));
      chunkInfo.setNumDocs(LogIndexSearcherImpl.getNumDocs(dataDirectory));
    } catch (IOException ignored) {
    }

    return chunkInfo;
  }

  public static SnapshotMetadata toSnapshotMetadata(ChunkInfo chunkInfo) {
    // TODO: Set the start offset for the kafka partition.
    // TODO: Pass in the snapshot path as input?
    return new SnapshotMetadata(
        chunkInfo.chunkId,
        SearchMetadata.LIVE_SNAPSHOT_NAME,
        chunkInfo.chunkId,
        chunkInfo.getDataStartTimeEpochMs(),
        chunkInfo.getDataEndTimeEpochMs(),
        chunkInfo.maxOffset,
        chunkInfo.kafkaPartitionId);
  }

  /* A unique identifier for a the chunk. */
  public final String chunkId;

  // The time when this chunk is created.
  private final long chunkCreationTimeEpochMs;
  private final String kafkaPartitionId;
  private long maxOffset;

  /*
   * The last time when this chunk is updated. Ideally, we want to set this timestamp continuously,
   * but fetching current timestamp for every message slows down indexing and this value is not that important.
   * So, we only set it once when the chunk is closed.
   */
  private long chunkLastUpdatedTimeEpochMs;

  /**
   * The dataStartTimeSecsSinceEpoch and dataEndTimeSecsSinceEpoch capture the time range of the
   * data in this chunk. NOTE: Ideally, we should make these fields optional but these are updated
   * in the hot ingestion path. So, keeping primitive types reduces allocation.
   */
  private long dataStartTimeEpochMs;

  private long dataEndTimeEpochMs;

  // This field contains the time the chunk is snapshotted. This info is used only during indexing
  // and snapshotting and is not useful afterwards.
  private long chunkSnapshotTimeEpochMs;

  public long getNumDocs() {
    return numDocs;
  }

  public void setNumDocs(long numDocs) {
    this.numDocs = numDocs;
  }

  public long getChunkSize() {
    return chunkSize;
  }

  public void setChunkSize(long chunkSize) {
    this.chunkSize = chunkSize;
  }

  // TODO: Why do we need this info?
  // Number of docs in this chunk
  private long numDocs;

  // Size of the chunk
  private long chunkSize;

  public ChunkInfo(String chunkId, long chunkCreationTimeEpochMs, String kafkaPartitionId) {
    // TODO: Should we set the snapshot time to creation time also?
    this(
        chunkId,
        chunkCreationTimeEpochMs,
        chunkCreationTimeEpochMs,
        chunkCreationTimeEpochMs,
        MAX_FUTURE_TIME,
        0,
        0,
        0,
        DEFAULT_MAX_OFFSET,
        kafkaPartitionId);
  }

  public ChunkInfo(
      String chunkId,
      long chunkCreationTimeEpochMs,
      long chunkLastUpdatedTimeEpochMs,
      long dataStartTimeEpochMs,
      long dataEndTimeEpochMs,
      long chunkSnapshotTimeEpochMs,
      long numDocs,
      long chunkSize,
      long maxOffset,
      String kafkaPartitionId) {
    ensureTrue(chunkId != null && !chunkId.isEmpty(), "Invalid chunk dataset name " + chunkId);
    ensureTrue(
        chunkCreationTimeEpochMs >= 0,
        "Chunk creation time should be non negative: " + chunkCreationTimeEpochMs);
    ensureTrue(kafkaPartitionId != null && !kafkaPartitionId.isEmpty(), "Invalid KafkaPartitionId");

    this.chunkId = chunkId;
    this.chunkCreationTimeEpochMs = chunkCreationTimeEpochMs;
    this.chunkLastUpdatedTimeEpochMs = chunkLastUpdatedTimeEpochMs;
    this.dataStartTimeEpochMs = dataStartTimeEpochMs;
    this.dataEndTimeEpochMs = dataEndTimeEpochMs;
    this.chunkSnapshotTimeEpochMs = chunkSnapshotTimeEpochMs;
    this.numDocs = numDocs;
    this.chunkSize = chunkSize;
    this.maxOffset = maxOffset;
    this.kafkaPartitionId = kafkaPartitionId;
  }

  public long getChunkSnapshotTimeEpochMs() {
    return chunkSnapshotTimeEpochMs;
  }

  public void setChunkSnapshotTimeEpochMs(long chunkSnapshotTimeEpochMs) {
    this.chunkSnapshotTimeEpochMs = chunkSnapshotTimeEpochMs;
  }

  public long getDataStartTimeEpochMs() {
    return dataStartTimeEpochMs;
  }

  public long getDataEndTimeEpochMs() {
    return dataEndTimeEpochMs;
  }

  public long getChunkCreationTimeEpochMs() {
    return chunkCreationTimeEpochMs;
  }

  public long getMaxOffset() {
    return maxOffset;
  }

  public long getChunkLastUpdatedTimeEpochMs() {
    return chunkLastUpdatedTimeEpochMs;
  }

  public void setChunkLastUpdatedTimeEpochMs(long chunkLastUpdatedTimeEpochMs) {
    this.chunkLastUpdatedTimeEpochMs = chunkLastUpdatedTimeEpochMs;
  }

  public void setDataStartTimeEpochMs(long dataStartTimeEpochMs) {
    this.dataStartTimeEpochMs = dataStartTimeEpochMs;
  }

  public void setDataEndTimeEpochMs(long dataEndTimeEpochMs) {
    this.dataEndTimeEpochMs = dataEndTimeEpochMs;
  }

  public void updateMaxOffset(long newOffset) {
    maxOffset = Math.max(maxOffset, newOffset);
  }

  // Return true if chunk contains data in this time range.
  public boolean containsDataInTimeRange(long startTimeMs, long endTimeMs) {
    ensureTrue(endTimeMs >= 0, "end timestamp should be greater than zero: " + endTimeMs);
    ensureTrue(startTimeMs >= 0, "start timestamp should be greater than zero: " + startTimeMs);
    ensureTrue(
        endTimeMs - startTimeMs >= 0,
        String.format(
            "end timestamp %d can't be less than the start timestamp %d.", endTimeMs, startTimeMs));
    return (dataStartTimeEpochMs <= startTimeMs && dataEndTimeEpochMs >= startTimeMs)
        || (dataStartTimeEpochMs <= endTimeMs && dataEndTimeEpochMs >= endTimeMs)
        || (dataStartTimeEpochMs >= startTimeMs && dataEndTimeEpochMs <= endTimeMs);
  }

  /*
   * Update the max and min data time range of the chunk given a new timestamp.
   */
  public void updateDataTimeRange(long messageTimeStampMs) {
    if (dataEndTimeEpochMs == MAX_FUTURE_TIME) {
      dataStartTimeEpochMs = Math.min(dataStartTimeEpochMs, messageTimeStampMs);
      dataEndTimeEpochMs = Math.min(dataEndTimeEpochMs, messageTimeStampMs);
    } else {
      // TODO: Would only updating the values if there is a change make this code faster?
      dataStartTimeEpochMs = Math.min(dataStartTimeEpochMs, messageTimeStampMs);
      dataEndTimeEpochMs = Math.max(dataEndTimeEpochMs, messageTimeStampMs);
    }
  }

  @Override
  public String toString() {
    return "ChunkInfo{"
        + "chunkId='"
        + chunkId
        + '\''
        + ", chunkCreationTimeEpochMs="
        + chunkCreationTimeEpochMs
        + ", chunkLastUpdatedTimeEpochMs="
        + chunkLastUpdatedTimeEpochMs
        + ", dataStartTimeEpochMs="
        + dataStartTimeEpochMs
        + ", dataEndTimeEpochMs="
        + dataEndTimeEpochMs
        + ", chunkSnapshotTimeEpochMs="
        + chunkSnapshotTimeEpochMs
        + ", numDocs="
        + numDocs
        + ", chunkSize="
        + chunkSize
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChunkInfo chunkInfo = (ChunkInfo) o;
    return chunkCreationTimeEpochMs == chunkInfo.chunkCreationTimeEpochMs
        && chunkLastUpdatedTimeEpochMs == chunkInfo.chunkLastUpdatedTimeEpochMs
        && dataStartTimeEpochMs == chunkInfo.dataStartTimeEpochMs
        && dataEndTimeEpochMs == chunkInfo.dataEndTimeEpochMs
        && chunkSnapshotTimeEpochMs == chunkInfo.chunkSnapshotTimeEpochMs
        && Objects.equals(chunkId, chunkInfo.chunkId)
        && numDocs == chunkInfo.numDocs
        && chunkSize == chunkInfo.chunkSize;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        chunkId,
        chunkCreationTimeEpochMs,
        chunkLastUpdatedTimeEpochMs,
        dataStartTimeEpochMs,
        dataEndTimeEpochMs,
        chunkSnapshotTimeEpochMs,
        numDocs,
        chunkSize);
  }
}
