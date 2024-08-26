package com.slack.astra.metadata.snapshot;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.astra.metadata.core.AstraPartitionedMetadata;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

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
public class SnapshotMetadata extends AstraPartitionedMetadata {
  public final String snapshotId;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;
  public final long maxOffset;
  public final String partitionId;
  public long sizeInBytesOnDisk;

  public SnapshotMetadata(
      String snapshotId,
      long startTimeEpochMs,
      long endTimeEpochMs,
      long maxOffset,
      String partitionId,
      long sizeInBytesOnDisk) {
    this(
        snapshotId,
        snapshotId,
        startTimeEpochMs,
        endTimeEpochMs,
        maxOffset,
        partitionId,
        sizeInBytesOnDisk);
  }

  private SnapshotMetadata(
      String name,
      String snapshotId,
      long startTimeEpochMs,
      long endTimeEpochMs,
      long maxOffset,
      String partitionId,
      long sizeInBytesOnDisk) {
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

    this.snapshotId = snapshotId;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.maxOffset = maxOffset;
    this.partitionId = partitionId;
    this.sizeInBytesOnDisk = sizeInBytesOnDisk;
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SnapshotMetadata that)) return false;
    if (!super.equals(o)) return false;

    return startTimeEpochMs == that.startTimeEpochMs
        && endTimeEpochMs == that.endTimeEpochMs
        && maxOffset == that.maxOffset
        && sizeInBytesOnDisk == that.sizeInBytesOnDisk
        && snapshotId.equals(that.snapshotId)
        && partitionId.equals(that.partitionId);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + snapshotId.hashCode();
    result = 31 * result + Long.hashCode(startTimeEpochMs);
    result = 31 * result + Long.hashCode(endTimeEpochMs);
    result = 31 * result + Long.hashCode(maxOffset);
    result = 31 * result + partitionId.hashCode();
    result = 31 * result + Long.hashCode(sizeInBytesOnDisk);
    return result;
  }

  @Override
  public String toString() {
    return "SnapshotMetadata{"
        + "snapshotId='"
        + snapshotId
        + '\''
        + ", startTimeEpochMs="
        + startTimeEpochMs
        + ", endTimeEpochMs="
        + endTimeEpochMs
        + ", maxOffset="
        + maxOffset
        + ", partitionId='"
        + partitionId
        + '\''
        + ", sizeInBytesOnDisk="
        + sizeInBytesOnDisk
        + ", name='"
        + name
        + '\''
        + '}';
  }

  @Override
  public String getPartition() {
    if (isLive()) {
      // this keeps all the live snapshots in a single partition - this is important as their stored
      // startTimeEpochMs is not stable, and will be updated. This would cause an update to a live
      // node to fail with a partitioned metadata store as it cannot change the path of the znode.
      return "LIVE";
    } else {
      ZonedDateTime snapshotTime = Instant.ofEpochMilli(startTimeEpochMs).atZone(ZoneOffset.UTC);
      return String.format(
          "%s_%s",
          snapshotTime.getLong(ChronoField.EPOCH_DAY),
          snapshotTime.getLong(ChronoField.HOUR_OF_DAY));
    }
  }

  // todo - this is better than the previous version of storing a static "LIVE" string to a path
  //  variable but not by a lot. The "isLive" functionality should be reconsidered more broadly.
  //  The ideal way is likely to reconsider the ZK type for "LIVE" snapshots
  public boolean isLive() {
    return this.sizeInBytesOnDisk == 0;
  }
}
