package com.slack.kaldb.metadata.replica;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import java.util.Objects;

/**
 * The replica metadata is used to allow associating multiple cache nodes to a single snapshot. The
 * cluster manager will create one (or more) replicas per snapshot, depending on configuration and
 * expected/observed query load.
 */
public class ReplicaMetadata extends KaldbMetadata {

  public final String snapshotId;
  public final long createdTimeEpochMs;
  public final long expireAfterEpochMs;
  public boolean isRestored;

  public ReplicaMetadata(
      String name, String snapshotId, long createdTimeEpochMs, long expireAfterEpochMs) {
    this(name, snapshotId, createdTimeEpochMs, expireAfterEpochMs, false);
  }

  public ReplicaMetadata(
      String name,
      String snapshotId,
      long createdTimeEpochMs,
      long expireAfterEpochMs,
      boolean isRestored) {
    super(name);
    checkArgument(createdTimeEpochMs > 0, "Created time must be greater than 0");
    checkArgument(expireAfterEpochMs >= 0, "Expiration time must be greater than or equal to 0");
    checkArgument(
        snapshotId != null && !snapshotId.isEmpty(), "SnapshotId must not be null or empty");

    this.snapshotId = snapshotId;
    this.createdTimeEpochMs = createdTimeEpochMs;
    this.expireAfterEpochMs = expireAfterEpochMs;
    this.isRestored = isRestored;
  }

  public String getSnapshotId() {
    return snapshotId;
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
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ReplicaMetadata that = (ReplicaMetadata) o;
    return createdTimeEpochMs == that.createdTimeEpochMs
        && expireAfterEpochMs == that.expireAfterEpochMs
        && snapshotId.equals(that.snapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), snapshotId, createdTimeEpochMs, expireAfterEpochMs);
  }

  @Override
  public String toString() {
    return "ReplicaMetadata{"
        + "name='"
        + name
        + '\''
        + ", snapshotId='"
        + snapshotId
        + '\''
        + ", createdTimeEpochMs="
        + createdTimeEpochMs
        + ", expireAfterEpochMs="
        + expireAfterEpochMs
        + ", isRestored="
        + isRestored
        + '}';
  }
}
