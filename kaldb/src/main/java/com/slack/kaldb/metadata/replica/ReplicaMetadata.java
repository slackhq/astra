package com.slack.kaldb.metadata.replica;

import com.slack.kaldb.metadata.core.KaldbMetadata;

public class ReplicaMetadata extends KaldbMetadata {

  public final String snapshotId;

  public ReplicaMetadata(String name, String snapshotId) {
    super(name);
    this.snapshotId = snapshotId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    ReplicaMetadata that = (ReplicaMetadata) o;

    return snapshotId.equals(that.snapshotId);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + snapshotId.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "ReplicaMetadata{" + "name='" + name + '\'' + ", snapshotId='" + snapshotId + '\'' + '}';
  }
}
