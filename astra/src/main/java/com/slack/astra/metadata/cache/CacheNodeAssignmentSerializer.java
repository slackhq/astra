package com.slack.astra.metadata.cache;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class CacheNodeAssignmentSerializer implements MetadataSerializer<CacheNodeAssignment> {
  private static CacheNodeAssignment fromCacheNodeAssignmentProto(
      Metadata.CacheNodeAssignment cacheSlotMetadataProto) {
    return new CacheNodeAssignment(
        cacheSlotMetadataProto.getAssignmentId(),
        cacheSlotMetadataProto.getCacheNodeId(),
        cacheSlotMetadataProto.getSnapshotId(),
        cacheSlotMetadataProto.getReplicaId(),
        cacheSlotMetadataProto.getReplicaSet(),
        cacheSlotMetadataProto.getSnapshotSize(),
        cacheSlotMetadataProto.getState());
  }

  private static Metadata.CacheNodeAssignment toCacheNodeAssignmentProto(
      CacheNodeAssignment metadata) {
    return Metadata.CacheNodeAssignment.newBuilder()
        .setAssignmentId(metadata.assignmentId)
        .setCacheNodeId(metadata.cacheNodeId)
        .setSnapshotId(metadata.snapshotId)
        .setReplicaId(metadata.replicaId)
        .setReplicaSet(metadata.replicaSet)
        .setState(metadata.state)
        .setSnapshotSize(metadata.snapshotSize)
        .build();
  }

  @Override
  public String toJsonStr(CacheNodeAssignment metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toCacheNodeAssignmentProto(metadata));
  }

  @Override
  public CacheNodeAssignment fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.CacheNodeAssignment.Builder cacheNodeMetadataBuilder =
        Metadata.CacheNodeAssignment.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, cacheNodeMetadataBuilder);
    return fromCacheNodeAssignmentProto(cacheNodeMetadataBuilder.build());
  }
}
