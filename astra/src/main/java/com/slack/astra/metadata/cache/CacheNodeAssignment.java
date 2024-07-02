package com.slack.astra.metadata.cache;

import com.slack.astra.metadata.core.AstraPartitionedMetadata;
import com.slack.astra.proto.metadata.Metadata;
import java.util.Objects;

public class CacheNodeAssignment extends AstraPartitionedMetadata {
  public final String assignmentId;
  public final String cacheNodeId;
  public final String snapshotId;
  public final String replicaSet;
  public Metadata.CacheNodeAssignment.CacheNodeAssignmentState state;

  public CacheNodeAssignment(
      String assignmentId,
      String cacheNodeId,
      String snapshotId,
      String replicaSet,
      Metadata.CacheNodeAssignment.CacheNodeAssignmentState state) {
    super(assignmentId);
    this.assignmentId = assignmentId;
    this.cacheNodeId = cacheNodeId;
    this.snapshotId = snapshotId;
    this.replicaSet = replicaSet;
    this.state = state;
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
    if (!Objects.equals(cacheNodeId, that.cacheNodeId)) return false;
    return Objects.equals(state, that.state);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + assignmentId.hashCode();
    result = 31 * result + cacheNodeId.hashCode();
    result = 31 * result + snapshotId.hashCode();
    result = 31 * result + state.hashCode();
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
        + ", state='"
        + state
        + '\''
        + '}';
  }
}
