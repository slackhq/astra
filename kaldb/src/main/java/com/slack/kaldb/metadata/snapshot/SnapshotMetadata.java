package com.slack.kaldb.metadata.snapshot;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbMetadata;

/**
 * The SnapshotMetadata class contains all the metadata related to a snapshot.
 *
 * <p>Currently, we also assume that the name of the node is the same as the snapshotId. We make
 * this distinction to allow a multiple snapshots to point to same data. However, we don't need that
 * functionality yet. So, we don't expose this flexibility to the application for now.
 *
 * <p>Note: Currently, we assume that we will always index partitions linearly. So, we only store
 * the endOffset for a Kafka partition. The starting offset would be the continuation from the
 * previous offset (except in case of a recovery task). Since this info is only used for debugging
 * for now, this should be fine. If this is inconvenient, consider adding a startOffset field also
 * here.
 */
public class SnapshotMetadata extends KaldbMetadata {
  public static final String LIVE_SNAPSHOT_PATH = "LIVE";

  public static boolean isLive(SnapshotMetadata snapshotMetadata) {
    return snapshotMetadata.snapshotPath.equals(LIVE_SNAPSHOT_PATH);
  }

  public final String snapshotPath;
  public final String snapshotId;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;
  public final long maxOffset;
  public final String partitionId;

  public SnapshotMetadata(
      String snapshotId,
      String snapshotPath,
      long startTimeEpochMs,
      long endTimeEpochMs,
      long maxOffset,
      String partitionId) {
    this(
        snapshotId,
        snapshotPath,
        snapshotId,
        startTimeEpochMs,
        endTimeEpochMs,
        maxOffset,
        partitionId);
  }

  private SnapshotMetadata(
      String name,
      String snapshotPath,
      String snapshotId,
      long startTimeEpochMs,
      long endTimeEpochMs,
      long maxOffset,
      String partitionId) {
    super(name);
    checkArgument(snapshotId != null && !snapshotId.isEmpty(), "snapshotId can't be null or empty");
    checkArgument(startTimeEpochMs > 0, "start time should be greater than zero.");
    checkArgument(endTimeEpochMs > 0, "end time should be greater than zero.");
    checkArgument(
        endTimeEpochMs >= startTimeEpochMs,
        "start time should be greater than or equal to endtime");
    checkArgument(maxOffset >= 0, "max offset should be greater than or equal to zero.");
    checkArgument(
        partitionId != null && !partitionId.isEmpty(), "partitionId can't be null or empty");
    checkArgument(
        snapshotPath != null && !snapshotPath.isEmpty(), "snapshotPath can't be null or empty");

    this.snapshotPath = snapshotPath;
    this.snapshotId = snapshotId;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.maxOffset = maxOffset;
    this.partitionId = partitionId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    SnapshotMetadata that = (SnapshotMetadata) o;

    if (startTimeEpochMs != that.startTimeEpochMs) return false;
    if (endTimeEpochMs != that.endTimeEpochMs) return false;
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
    result = 31 * result + (int) (startTimeEpochMs ^ (startTimeEpochMs >>> 32));
    result = 31 * result + (int) (endTimeEpochMs ^ (endTimeEpochMs >>> 32));
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
        + ", startTimeEpochMsUtc="
        + startTimeEpochMs
        + ", endTimeEpochMsUtc="
        + endTimeEpochMs
        + ", maxOffset="
        + maxOffset
        + ", partitionId='"
        + partitionId
        + '\''
        + '}';
  }
}
