package com.slack.astra.metadata.cache;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.proto.metadata.Metadata;
import org.junit.jupiter.api.Test;

public class CacheNodeAssignmentTest {
  @Test
  public void testCacheNodeAssignment() {
    String assignmentId = "assignmentId";
    String cacheNodeId = "node1";
    String snapshotId = "snapshotId";
    String replicaId = "replicaId";
    String replicaSet = "rep2";
    long snapshotSize = 1024;
    Metadata.CacheNodeAssignment.CacheNodeAssignmentState state =
        Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING;

    CacheNodeAssignment cacheNodeAssignment =
        new CacheNodeAssignment(
            assignmentId, cacheNodeId, snapshotId, replicaId, replicaSet, snapshotSize, state);
    assertThat(cacheNodeAssignment.assignmentId).isEqualTo(assignmentId);
    assertThat(cacheNodeAssignment.cacheNodeId).isEqualTo(cacheNodeId);
    assertThat(cacheNodeAssignment.snapshotId).isEqualTo(snapshotId);
    assertThat(cacheNodeAssignment.replicaId).isEqualTo(replicaId);
    assertThat(cacheNodeAssignment.replicaSet).isEqualTo(replicaSet);
    assertThat(cacheNodeAssignment.snapshotSize).isEqualTo(snapshotSize);
    assertThat(cacheNodeAssignment.state).isEqualTo(state);
  }

  @Test
  public void testCacheNodeAssignmentEqualsHashcode() {
    String assignmentId = "assignmentId";
    String cacheNodeId = "node1";
    String snapshotId = "snapshotId";
    String replicaId = "replicaId";
    String replicaSet = "rep2";
    long snapshotSize = 1024;
    Metadata.CacheNodeAssignment.CacheNodeAssignmentState state =
        Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING;

    CacheNodeAssignment cacheNodeAssignment =
        new CacheNodeAssignment(
            assignmentId, cacheNodeId, snapshotId, replicaId, replicaSet, snapshotSize, state);
    CacheNodeAssignment cacheNodeAssignmentDuplicate =
        new CacheNodeAssignment(
            assignmentId, cacheNodeId, snapshotId, replicaId, replicaSet, snapshotSize, state);
    CacheNodeAssignment cacheNodeAssignmentDifferentId =
        new CacheNodeAssignment(
            "foo", cacheNodeId, snapshotId, replicaId, replicaSet, snapshotSize, state);
    CacheNodeAssignment cacheNodeAssignmentDifferentCacheNode =
        new CacheNodeAssignment(
            assignmentId, "foo", snapshotId, replicaId, replicaSet, snapshotSize, state);
    CacheNodeAssignment cacheNodeAssignmentDifferentSnapshot =
        new CacheNodeAssignment(
            assignmentId, cacheNodeId, "foo", replicaId, replicaSet, snapshotSize, state);
    CacheNodeAssignment cacheNodeAssignmentDifferentReplica =
        new CacheNodeAssignment(
            assignmentId, cacheNodeId, snapshotId, "foo", replicaSet, snapshotSize, state);
    CacheNodeAssignment cacheNodeAssignmentDifferentReplicaSet =
        new CacheNodeAssignment(
            assignmentId, cacheNodeId, snapshotId, replicaId, "foo", snapshotSize, state);
    CacheNodeAssignment cacheNodeAssignmentDifferentSize =
        new CacheNodeAssignment(
            assignmentId, cacheNodeId, snapshotId, replicaId, replicaSet, 1, state);
    CacheNodeAssignment cacheNodeAssignmentDifferentState =
        new CacheNodeAssignment(
            assignmentId,
            cacheNodeId,
            snapshotId,
            replicaId,
            replicaSet,
            snapshotSize,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE);

    assertThat(cacheNodeAssignment.hashCode()).isEqualTo(cacheNodeAssignmentDuplicate.hashCode());
    assertThat(cacheNodeAssignment).isEqualTo(cacheNodeAssignmentDuplicate);

    assertThat(cacheNodeAssignment).isNotEqualTo(cacheNodeAssignmentDifferentId);
    assertThat(cacheNodeAssignment.hashCode())
        .isNotEqualTo(cacheNodeAssignmentDifferentId.hashCode());
    assertThat(cacheNodeAssignment).isNotEqualTo(cacheNodeAssignmentDifferentCacheNode);
    assertThat(cacheNodeAssignment.hashCode())
        .isNotEqualTo(cacheNodeAssignmentDifferentCacheNode.hashCode());
    assertThat(cacheNodeAssignment).isNotEqualTo(cacheNodeAssignmentDifferentSnapshot);
    assertThat(cacheNodeAssignment.hashCode())
        .isNotEqualTo(cacheNodeAssignmentDifferentSnapshot.hashCode());
    assertThat(cacheNodeAssignment).isNotEqualTo(cacheNodeAssignmentDifferentReplica);
    assertThat(cacheNodeAssignment.hashCode())
        .isNotEqualTo(cacheNodeAssignmentDifferentReplica.hashCode());
    assertThat(cacheNodeAssignment).isNotEqualTo(cacheNodeAssignmentDifferentReplicaSet);
    assertThat(cacheNodeAssignment.hashCode())
        .isNotEqualTo(cacheNodeAssignmentDifferentReplicaSet.hashCode());
    assertThat(cacheNodeAssignment).isNotEqualTo(cacheNodeAssignmentDifferentSize);
    assertThat(cacheNodeAssignment.hashCode())
        .isNotEqualTo(cacheNodeAssignmentDifferentSize.hashCode());
    assertThat(cacheNodeAssignment).isNotEqualTo(cacheNodeAssignmentDifferentState);
    assertThat(cacheNodeAssignment.hashCode())
        .isNotEqualTo(cacheNodeAssignmentDifferentState.hashCode());
  }
}
