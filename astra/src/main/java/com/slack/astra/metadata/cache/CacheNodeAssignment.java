package com.slack.astra.metadata.cache;

import com.slack.astra.metadata.core.AstraPartitionedMetadata;
import com.slack.astra.proto.metadata.Metadata;
import java.util.Objects;

public class CacheNodeAssignment extends AstraPartitionedMetadata {
  public final String assignmentId;
  public final String cacheNodeId;
  public final String snapshotId;
  public final String replicaId;
  public final String replicaSet;
  public final long snapshotSize;
  public Metadata.CacheNodeAssignment.CacheNodeAssignmentState state;

  public CacheNodeAssignment(
      String assignmentId,
      String cacheNodeId,
      String snapshotId,
      String replicaId,
      String replicaSet,
      long snapshotSize,
      Metadata.CacheNodeAssignment.CacheNodeAssignmentState state) {
    super(assignmentId);
    this.assignmentId = assignmentId;
    this.cacheNodeId = cacheNodeId;
    this.snapshotId = snapshotId;
    this.replicaId = replicaId;
    this.replicaSet = replicaSet;
    this.state = state;
    this.snapshotSize = snapshotSize;
  }

  @Override
  public String getPartition() {
    return cacheNodeId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CacheNodeAssignment that)) return false;
    if (!super.equals(o)) return false;

    if (!assignmentId.equals(that.assignmentId)) return false;
    if (!snapshotId.equals(that.snapshotId)) return false;
    if (!replicaId.equals(that.replicaId)) return false;
    if (!replicaSet.equals(that.replicaSet)) return false;
    if (!Objects.equals(cacheNodeId, that.cacheNodeId)) return false;
    if (!(snapshotSize == that.snapshotSize)) return false;
    return Objects.equals(state, that.state);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + assignmentId.hashCode();
    result = 31 * result + cacheNodeId.hashCode();
    result = 31 * result + replicaId.hashCode();
    result = 31 * result + replicaSet.hashCode();
    result = 31 * result + snapshotId.hashCode();
    result = 31 * result + state.hashCode();
    result = 31 * result + Long.hashCode(snapshotSize);
    return result;
  }

  @Override
  public String toString() {
    return "CacheNodeAssignment{"
        + "assignmentId='"
        + assignmentId
        + '\''
        + ", cacheNodeId='"
        + cacheNodeId
        + '\''
        + ", snapshotId="
        + snapshotId
        + ", replicaId="
        + replicaId
        + ", state='"
        + state
        + ", replicaSet='"
        + replicaSet
        + '\''
        + '}';
  }
}
