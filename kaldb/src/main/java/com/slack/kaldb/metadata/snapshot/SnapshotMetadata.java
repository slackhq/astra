package com.slack.kaldb.metadata.snapshot;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbPartitionedMetadata;
import com.slack.kaldb.proto.metadata.Metadata;
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
public class SnapshotMetadata extends KaldbPartitionedMetadata {
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
  public final Metadata.IndexType indexType;

  public SnapshotMetadata(
      String snapshotId,
      String snapshotPath,
      long startTimeEpochMs,
      long endTimeEpochMs,
      long maxOffset,
      String partitionId,
      Metadata.IndexType indexType) {
    this(
        snapshotId,
        snapshotPath,
        snapshotId,
        startTimeEpochMs,
        endTimeEpochMs,
        maxOffset,
        partitionId,
        indexType);
  }

  private SnapshotMetadata(
      String name,
      String snapshotPath,
      String snapshotId,
      long startTimeEpochMs,
      long endTimeEpochMs,
      long maxOffset,
      String partitionId,
      Metadata.IndexType indexType) {
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
    this.indexType = indexType;
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
    if (snapshotPath != null ? !snapshotPath.equals(that.snapshotPath) : that.snapshotPath != null)
      return false;
    if (snapshotId != null ? !snapshotId.equals(that.snapshotId) : that.snapshotId != null)
      return false;
    if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null)
      return false;
    return indexType == that.indexType;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (snapshotPath != null ? snapshotPath.hashCode() : 0);
    result = 31 * result + (snapshotId != null ? snapshotId.hashCode() : 0);
    result = 31 * result + (int) (startTimeEpochMs ^ (startTimeEpochMs >>> 32));
    result = 31 * result + (int) (endTimeEpochMs ^ (endTimeEpochMs >>> 32));
    result = 31 * result + (int) (maxOffset ^ (maxOffset >>> 32));
    result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
    result = 31 * result + (indexType != null ? indexType.hashCode() : 0);
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
        + ", startTimeEpochMs="
        + startTimeEpochMs
        + ", endTimeEpochMs="
        + endTimeEpochMs
        + ", maxOffset="
        + maxOffset
        + ", partitionId='"
        + partitionId
        + '\''
        + ", indexType="
        + indexType
        + '}';
  }

  @Override
  public String nodeName() {
    return name;
  }

  @Override
  public String getPartition() {
    if (isLive(this)) {
      return "LIVE";
    } else {
      ZonedDateTime snapshotTime = Instant.ofEpochMilli(startTimeEpochMs).atZone(ZoneOffset.UTC);
      return String.format(
          "%s_%s",
          snapshotTime.getLong(ChronoField.EPOCH_DAY),
          snapshotTime.getLong(ChronoField.HOUR_OF_DAY));
    }
  }
}
