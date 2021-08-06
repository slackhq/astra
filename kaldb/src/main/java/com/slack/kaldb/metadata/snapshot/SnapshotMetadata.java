package com.slack.kaldb.metadata.snapshot;

import static com.google.common.base.Preconditions.checkState;

import com.slack.kaldb.metadata.core.KaldbMetadata;

public class SnapshotMetadata extends KaldbMetadata {

  public final String snapshotPath;
  public final String snapshotId;
  public final long startTimeUtc;
  public final long endTimeUtc;
  public final long maxOffset;
  public final String partitionId;

  public SnapshotMetadata(
      String name,
      String snapshotPath,
      String snapshotId,
      long startTimeUtc,
      long endTimeUtc,
      long maxOffset,
      String partitionId) {
    super(name);

    checkState(
        snapshotPath != null && !snapshotPath.isEmpty(), "snapshotPath can't be null or empty");
    checkState(snapshotId != null && !snapshotId.isEmpty(), "snapshotId can't be null or empty");
    checkState(startTimeUtc > 0, "start time should be greater than zero.");
    checkState(endTimeUtc > 0, "end time should be greater than zero.");
    checkState(endTimeUtc > startTimeUtc, "start time should be greater than  end time.");
    checkState(maxOffset > 0, "max offset should be greater than zero.");
    checkState(partitionId != null && !partitionId.isEmpty(), "partitionId can't be null or empty");

    this.snapshotPath = snapshotPath;
    this.snapshotId = snapshotId;
    this.startTimeUtc = startTimeUtc;
    this.endTimeUtc = endTimeUtc;
    this.maxOffset = maxOffset;
    this.partitionId = partitionId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    SnapshotMetadata that = (SnapshotMetadata) o;

    if (startTimeUtc != that.startTimeUtc) return false;
    if (endTimeUtc != that.endTimeUtc) return false;
    if (maxOffset != that.maxOffset) return false;
    if (!snapshotPath.equals(that.snapshotPath)) return false;
    if (!snapshotId.equals(that.snapshotId)) return false;
    return partitionId.equals(that.partitionId);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + snapshotPath.hashCode();
    result = 31 * result + snapshotId.hashCode();
    result = 31 * result + (int) (startTimeUtc ^ (startTimeUtc >>> 32));
    result = 31 * result + (int) (endTimeUtc ^ (endTimeUtc >>> 32));
    result = 31 * result + (int) (maxOffset ^ (maxOffset >>> 32));
    result = 31 * result + partitionId.hashCode();
    return result;
  }

  @Override
  public String toString() {
    // Include name from super class in the toString method to simplify debugging.
    return "SnapshotMetadata{"
        + "name='"
        + name
        + '\''
        + ", snapshotPath='"
        + snapshotPath
        + '\''
        + ", snapshotId='"
        + snapshotId
        + '\''
        + ", startTimeUtc="
        + startTimeUtc
        + ", endTimeUtc="
        + endTimeUtc
        + ", maxOffset="
        + maxOffset
        + ", partitionId='"
        + partitionId
        + '\''
        + '}';
  }
}
