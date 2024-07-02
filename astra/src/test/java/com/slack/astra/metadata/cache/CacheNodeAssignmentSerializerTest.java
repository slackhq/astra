package com.slack.astra.metadata.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.metadata.Metadata;
import org.junit.jupiter.api.Test;

public class CacheNodeAssignmentSerializerTest {
  private final CacheNodeAssignmentSerializer serDe = new CacheNodeAssignmentSerializer();

  @Test
  public void testCacheNodeAssignmentSerializer() throws InvalidProtocolBufferException {
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

    String serializedCacheNodeAssignment = serDe.toJsonStr(cacheNodeAssignment);
    assertThat(serializedCacheNodeAssignment).isNotEmpty();

    CacheNodeAssignment deserializedCacheNodeAssignment =
        serDe.fromJsonStr(serializedCacheNodeAssignment);
    assertThat(deserializedCacheNodeAssignment).isEqualTo(cacheNodeAssignment);

    assertThat(deserializedCacheNodeAssignment.assignmentId).isEqualTo(assignmentId);
    assertThat(deserializedCacheNodeAssignment.cacheNodeId).isEqualTo(cacheNodeId);
    assertThat(deserializedCacheNodeAssignment.snapshotId).isEqualTo(snapshotId);
    assertThat(deserializedCacheNodeAssignment.replicaId).isEqualTo(replicaId);
    assertThat(deserializedCacheNodeAssignment.replicaSet).isEqualTo(replicaSet);
    assertThat(deserializedCacheNodeAssignment.state).isEqualTo(state);
    assertThat(deserializedCacheNodeAssignment.snapshotSize).isEqualTo(snapshotSize);
  }

  @Test
  public void testInvalidSerializations() {
    Throwable serializeNull = catchThrowable(() -> serDe.toJsonStr(null));
    assertThat(serializeNull).isInstanceOf(IllegalArgumentException.class);

    Throwable deserializeNull = catchThrowable(() -> serDe.fromJsonStr(null));
    assertThat(deserializeNull).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeEmpty = catchThrowable(() -> serDe.fromJsonStr(""));
    assertThat(deserializeEmpty).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeCorrupt = catchThrowable(() -> serDe.fromJsonStr("test"));
    assertThat(deserializeCorrupt).isInstanceOf(InvalidProtocolBufferException.class);
  }
}
