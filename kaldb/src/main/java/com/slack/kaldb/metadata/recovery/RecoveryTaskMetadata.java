package com.slack.kaldb.metadata.recovery;

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

  public RecoveryTaskMetadata(String name, String partitionId, long startOffset, long endOffset) {
    super(name);
    this.partitionId = partitionId;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    RecoveryTaskMetadata that = (RecoveryTaskMetadata) o;
    return startOffset == that.startOffset
        && endOffset == that.endOffset
        && partitionId.equals(that.partitionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), partitionId, startOffset, endOffset);
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
        + '}';
  }
}
