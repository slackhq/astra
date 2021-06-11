package com.slack.kaldb.chunk;

import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

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

  /* A unique identifier for a the chunk. */
  public final String chunkId;

  // The time when this chunk is created.
  private final long chunkCreationTimeEpochSecs;

  /*
   * The last time when this chunk is updated. Ideally, we want to set this timestamp continuously,
   * but fetching current timestamp for every message slows down indexing and this value is not that important.
   * So, we only set it once when the chunk is closed.
   */
  private long chunkLastUpdatedTimeSecsEpochSecs;

  /**
   * The dataStartTimeSecsSinceEpoch and dataEndTimeSecsSinceEpoch capture the time range of the
   * data in this chunk. NOTE: Ideally, we should make these fields optional but these are updated
   * in the hot ingestion path. So, keeping primitive types reduces allocation.
   */
  private long dataStartTimeEpochSecs;

  private long dataEndTimeEpochSecs;

  // This field contains the time the chunk is snapshotted. This info is used only during indexing
  // and snapshotting and is not useful afterwards.
  private long chunkSnapshotTimeEpochSecs;

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

  // Number of docs in this chunk
  private long numDocs;

  // Size of the chunk
  private long chunkSize;

  public ChunkInfo(String chunkId, long chunkCreationTimeEpochSecs) {
    ensureTrue(chunkId != null && !chunkId.isEmpty(), "Invalid chunk dataset name " + chunkId);
    ensureTrue(
        chunkCreationTimeEpochSecs >= 0,
        "Chunk creation time should be non negative: " + chunkCreationTimeEpochSecs);

    this.chunkId = chunkId;
    this.chunkCreationTimeEpochSecs = chunkCreationTimeEpochSecs;
    dataStartTimeEpochSecs = 0;
    dataEndTimeEpochSecs = 0;
    chunkLastUpdatedTimeSecsEpochSecs = chunkCreationTimeEpochSecs;
    // TODO: Should we set the snapshot time to creation time also?
    chunkSnapshotTimeEpochSecs = 0;
  }

  public long getChunkSnapshotTimeEpochSecs() {
    return chunkSnapshotTimeEpochSecs;
  }

  public void setChunkSnapshotTimeEpochSecs(long chunkSnapshotTimeEpochSecs) {
    this.chunkSnapshotTimeEpochSecs = chunkSnapshotTimeEpochSecs;
  }

  public long getDataStartTimeEpochSecs() {
    return dataStartTimeEpochSecs;
  }

  public long getDataEndTimeEpochSecs() {
    return dataEndTimeEpochSecs;
  }

  public long getChunkCreationTimeEpochSecs() {
    return chunkCreationTimeEpochSecs;
  }

  public long getChunkLastUpdatedTimeSecsEpochSecs() {
    return chunkLastUpdatedTimeSecsEpochSecs;
  }

  public void setChunkLastUpdatedTimeSecsEpochSecs(long chunkLastUpdatedTimeSecsEpochSecs) {
    this.chunkLastUpdatedTimeSecsEpochSecs = chunkLastUpdatedTimeSecsEpochSecs;
  }

  // Return true if chunk contains data in this time range.
  public boolean containsDataInTimeRange(long startTs, long endTs) {
    ensureTrue(endTs >= 0, "end timestamp should be greater than zero: " + endTs);
    ensureTrue(startTs >= 0, "start timestamp should be greater than zero: " + startTs);
    ensureTrue(
        endTs - startTs >= 0,
        String.format(
            "end timestamp %d can't be less than the start timestamp %d.", endTs, startTs));
    if (dataStartTimeEpochSecs == 0 || dataEndTimeEpochSecs == 0) {
      throw new IllegalStateException("Data start or end time should be initialized before query.");
    }
    return (dataStartTimeEpochSecs <= startTs && dataEndTimeEpochSecs >= startTs)
        || (dataStartTimeEpochSecs <= endTs && dataEndTimeEpochSecs >= endTs)
        || (dataStartTimeEpochSecs >= startTs && dataEndTimeEpochSecs <= endTs);
  }

  /*
   * Update the max and min data time range of the chunk given a new timestamp.
   */
  public void updateDataTimeRange(long messageTimeStampMs) {
    long messageTimeStampSecs = messageTimeStampMs / 1000;
    if (dataStartTimeEpochSecs == 0 || dataEndTimeEpochSecs == 0) {
      dataStartTimeEpochSecs = messageTimeStampSecs;
      dataEndTimeEpochSecs = messageTimeStampSecs;
    } else {
      // TODO: Would only updating the values if there is a change make this code faster?
      dataStartTimeEpochSecs = Math.min(dataStartTimeEpochSecs, messageTimeStampSecs);
      dataEndTimeEpochSecs = Math.max(dataEndTimeEpochSecs, messageTimeStampSecs);
    }
  }

  @Override
  public String toString() {
    return "ChunkInfo{"
        + "chunkId='"
        + chunkId
        + '\''
        + ", chunkCreationTimeSecsSinceEpoch="
        + chunkCreationTimeEpochSecs
        + ", chunkLastUpdatedTimeSecsSinceEpoch="
        + chunkLastUpdatedTimeSecsEpochSecs
        + ", dataStartTimeSecsSinceEpoch="
        + dataStartTimeEpochSecs
        + ", dataEndTimeSecsSinceEpoch="
        + dataEndTimeEpochSecs
        + ", chunkSnapshotTime="
        + chunkSnapshotTimeEpochSecs
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
    return chunkCreationTimeEpochSecs == chunkInfo.chunkCreationTimeEpochSecs
        && chunkLastUpdatedTimeSecsEpochSecs == chunkInfo.chunkLastUpdatedTimeSecsEpochSecs
        && dataStartTimeEpochSecs == chunkInfo.dataStartTimeEpochSecs
        && dataEndTimeEpochSecs == chunkInfo.dataEndTimeEpochSecs
        && chunkSnapshotTimeEpochSecs == chunkInfo.chunkSnapshotTimeEpochSecs
        && Objects.equals(chunkId, chunkInfo.chunkId)
        && numDocs == chunkInfo.numDocs
        && chunkSize == chunkInfo.chunkSize;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        chunkId,
        chunkCreationTimeEpochSecs,
        chunkLastUpdatedTimeSecsEpochSecs,
        dataStartTimeEpochSecs,
        dataEndTimeEpochSecs,
        chunkSnapshotTimeEpochSecs,
        numDocs,
        chunkSize);
  }
}
