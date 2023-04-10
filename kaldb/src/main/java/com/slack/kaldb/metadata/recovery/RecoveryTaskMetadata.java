package com.slack.kaldb.metadata.recovery;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.proto.metadata.Metadata;
import java.util.Objects;

/**
 * The recovery task metadata contains all information required to back-fill messages that have been
 * previously skipped. For partitionId, the recovery task should index from startOffset to
 * endOffset: [startOffset, endOffset].
 */
public class RecoveryTaskMetadata extends KaldbMetadata {
  public final String partitionId;
  public final long startOffset;
  public final long endOffset;
  public final long createdTimeEpochMs;
  public final Metadata.IndexType indexType;

  public RecoveryTaskMetadata(
      String name,
      String partitionId,
      long startOffset,
      long endOffset,
      Metadata.IndexType indexType,
      long createdTimeEpochMs) {
    super(name);

    checkArgument(
        partitionId != null && !partitionId.isEmpty(), "partitionId can't be null or empty");
    checkArgument(startOffset >= 0, "startOffset must greater than 0");
    checkArgument(
        endOffset >= startOffset, "endOffset must be greater than or equal to the startOffset");
    checkArgument(createdTimeEpochMs > 0, "createdTimeEpochMs must be greater than 0");
    checkArgument(indexType != null, "Index type can't be null");

    this.partitionId = partitionId;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.indexType = indexType;
    this.createdTimeEpochMs = createdTimeEpochMs;
  }

  public long getCreatedTimeEpochMs() {
    return createdTimeEpochMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RecoveryTaskMetadata)) return false;
    if (!super.equals(o)) return false;

    RecoveryTaskMetadata that = (RecoveryTaskMetadata) o;

    if (startOffset != that.startOffset) return false;
    if (endOffset != that.endOffset) return false;
    if (createdTimeEpochMs != that.createdTimeEpochMs) return false;
    if (!Objects.equals(partitionId, that.partitionId)) return false;
    return indexType == that.indexType;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
    result = 31 * result + (int) (startOffset ^ (startOffset >>> 32));
    result = 31 * result + (int) (endOffset ^ (endOffset >>> 32));
    result = 31 * result + (int) (createdTimeEpochMs ^ (createdTimeEpochMs >>> 32));
    result = 31 * result + (indexType != null ? indexType.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "RecoveryTaskMetadata{"
        + "partitionId='"
        + partitionId
        + '\''
        + ", startOffset="
        + startOffset
        + ", endOffset="
        + endOffset
        + ", createdTimeEpochMs="
        + createdTimeEpochMs
        + ", indexType="
        + indexType
        + ", name='"
        + name
        + '\''
        + '}';
  }
}
