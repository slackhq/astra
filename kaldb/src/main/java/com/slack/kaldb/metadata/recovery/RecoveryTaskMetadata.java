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
  public final long createdTimeUtc;

  public RecoveryTaskMetadata(
      String name, String partitionId, long startOffset, long endOffset, long createdTimeUtc) {
    super(name);

    checkArgument(
        partitionId != null && !partitionId.isEmpty(), "partitionId can't be null or empty");
    checkArgument(startOffset >= 0, "startOffset must greater than 0");
    checkArgument(endOffset > startOffset, "endOffset must be greater than the startOffset");
    checkArgument(createdTimeUtc > 0, "createdTimeUtc must be greater than 0");

    this.partitionId = partitionId;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.createdTimeUtc = createdTimeUtc;
  }

  public long getCreatedTimeUtc() {
    return createdTimeUtc;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    RecoveryTaskMetadata that = (RecoveryTaskMetadata) o;
    return startOffset == that.startOffset
        && endOffset == that.endOffset
        && createdTimeUtc == that.createdTimeUtc
        && partitionId.equals(that.partitionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), partitionId, startOffset, endOffset, createdTimeUtc);
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
        + createdTimeUtc
        + '}';
  }
}
