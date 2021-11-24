package com.slack.kaldb.metadata.replica;

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

  public ReplicaMetadata(String name, String snapshotId, long createdTimeUtc) {
    super(name);
    this.snapshotId = snapshotId;
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
    ReplicaMetadata that = (ReplicaMetadata) o;
    return createdTimeUtc == that.createdTimeUtc && snapshotId.equals(that.snapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), snapshotId, createdTimeUtc);
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
        + '}';
  }
}
