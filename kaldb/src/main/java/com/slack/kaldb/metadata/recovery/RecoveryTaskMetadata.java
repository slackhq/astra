package com.slack.kaldb.metadata.recovery;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import java.util.Objects;

/**
 * The recovery task metadata contains all information required to back-fill messages that have been
 * previously skipped.
 */
public class RecoveryTaskMetadata extends KaldbMetadata {
  public final String partitionId;
  public final long startOffset;
  public final long endOffset;
  public final long createdTimeEpochMsUtc;

  public RecoveryTaskMetadata(
      String name,
      String partitionId,
      long startOffset,
      long endOffset,
      long createdTimeEpochMsUtc) {
    super(name);

    checkArgument(
        partitionId != null && !partitionId.isEmpty(), "partitionId can't be null or empty");
    checkArgument(startOffset >= 0, "startOffset must greater than 0");
    checkArgument(endOffset > startOffset, "endOffset must be greater than the startOffset");
    checkArgument(createdTimeEpochMsUtc > 0, "createdTimeEpochMsUtc must be greater than 0");

    this.partitionId = partitionId;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.createdTimeEpochMsUtc = createdTimeEpochMsUtc;
  }

  public long getCreatedTimeEpochMsUtc() {
    return createdTimeEpochMsUtc;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    RecoveryTaskMetadata that = (RecoveryTaskMetadata) o;
    return startOffset == that.startOffset
        && endOffset == that.endOffset
        && createdTimeEpochMsUtc == that.createdTimeEpochMsUtc
        && partitionId.equals(that.partitionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), partitionId, startOffset, endOffset, createdTimeEpochMsUtc);
  }

  @Override
  public String toString() {
    return "RecoveryTaskMetadata{"
        + "name='"
        + name
        + '\''
        + ", partitionId='"
        + partitionId
        + '\''
        + ", startOffset="
        + startOffset
        + ", endOffset="
        + endOffset
        + ", createdTimeUtc="
        + createdTimeEpochMsUtc
        + '}';
  }
}
