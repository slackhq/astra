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
  public final long createdTimeUtc;
  public final long expireAfterUtc;

  public ReplicaMetadata(String name, String snapshotId, long createdTimeUtc, long expireAfterUtc) {
    super(name);
    checkArgument(createdTimeUtc > 0, "Created time must be greater than 0");
    checkArgument(expireAfterUtc >= 0, "Expiration time must be greater than or equal to 0");
    checkArgument(
        snapshotId != null && !snapshotId.isEmpty(), "SnapshotId must not be null or empty");

    this.snapshotId = snapshotId;
    this.createdTimeUtc = createdTimeUtc;
    this.expireAfterUtc = expireAfterUtc;
  }

  public String getSnapshotId() {
    return snapshotId;
  }

  public long getCreatedTimeUtc() {
    return createdTimeUtc;
  }

  public long getExpireAfterUtc() {
    return expireAfterUtc;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ReplicaMetadata that = (ReplicaMetadata) o;
    return createdTimeUtc == that.createdTimeUtc
        && expireAfterUtc == that.expireAfterUtc
        && snapshotId.equals(that.snapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), snapshotId, createdTimeUtc, expireAfterUtc);
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
        + ", createdTimeUtc="
        + createdTimeUtc
        + ", expireAfterUtc="
        + expireAfterUtc
        + '}';
  }
}
