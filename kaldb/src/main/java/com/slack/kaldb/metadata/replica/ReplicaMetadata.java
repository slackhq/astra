package com.slack.kaldb.metadata.replica;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbPartitionedMetadata;
import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

/**
 * The replica metadata is used to allow associating multiple cache nodes to a single snapshot. The
 * cluster manager will create one (or more) replicas per snapshot, depending on configuration and
 * expected/observed query load.
 */
public class ReplicaMetadata extends KaldbPartitionedMetadata {

  public final String snapshotId;

  public final String replicaSet;

  public final long createdTimeEpochMs;
  public final long expireAfterEpochMs;
  public boolean isRestored;
  public final Metadata.IndexType indexType;

  public ReplicaMetadata(
      String name,
      String snapshotId,
      String replicaSet,
      long createdTimeEpochMs,
      long expireAfterEpochMs,
      boolean isRestored,
      Metadata.IndexType indexType) {
    super(name);
    checkArgument(createdTimeEpochMs > 0, "Created time must be greater than 0");
    checkArgument(expireAfterEpochMs >= 0, "Expiration time must be greater than or equal to 0");
    checkArgument(
        snapshotId != null && !snapshotId.isEmpty(), "SnapshotId must not be null or empty");

    this.snapshotId = snapshotId;
    this.replicaSet = replicaSet;
    this.createdTimeEpochMs = createdTimeEpochMs;
    this.expireAfterEpochMs = expireAfterEpochMs;
    this.isRestored = isRestored;
    this.indexType = indexType;
  }

  public String getSnapshotId() {
    return snapshotId;
  }

  public String getReplicaSet() {
    return replicaSet;
  }

  public long getCreatedTimeEpochMs() {
    return createdTimeEpochMs;
  }

  public long getExpireAfterEpochMs() {
    return expireAfterEpochMs;
  }

  public boolean getIsRestored() {
    return isRestored;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ReplicaMetadata that)) return false;
    if (!super.equals(o)) return false;

    if (createdTimeEpochMs != that.createdTimeEpochMs) return false;
    if (expireAfterEpochMs != that.expireAfterEpochMs) return false;
    if (isRestored != that.isRestored) return false;
    if (!snapshotId.equals(that.snapshotId)) return false;
    if (!replicaSet.equals(that.replicaSet)) return false;
    return indexType == that.indexType;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + snapshotId.hashCode();
    result = 31 * result + replicaSet.hashCode();
    result = 31 * result + (int) (createdTimeEpochMs ^ (createdTimeEpochMs >>> 32));
    result = 31 * result + (int) (expireAfterEpochMs ^ (expireAfterEpochMs >>> 32));
    result = 31 * result + (isRestored ? 1 : 0);
    result = 31 * result + indexType.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "ReplicaMetadata{"
        + "snapshotId='"
        + snapshotId
        + '\''
        + ", replicaSet='"
        + replicaSet
        + '\''
        + ", createdTimeEpochMs="
        + createdTimeEpochMs
        + ", expireAfterEpochMs="
        + expireAfterEpochMs
        + ", isRestored="
        + isRestored
        + ", indexType="
        + indexType
        + ", name='"
        + name
        + '\''
        + '}';
  }

  @Override
  public String getPartition() {
    // We use the expireAfterEpochMs as this is derived from the snapshot end time. This ensures a
    // better distribution of snapshots in the event of a major recovery of replica nodes (ie, path
    // deleted)
    ZonedDateTime snapshotTime = Instant.ofEpochMilli(expireAfterEpochMs).atZone(ZoneOffset.UTC);
    return String.format(
        "%s_%s",
        snapshotTime.getLong(ChronoField.EPOCH_DAY), snapshotTime.getLong(ChronoField.HOUR_OF_DAY));
  }
}
