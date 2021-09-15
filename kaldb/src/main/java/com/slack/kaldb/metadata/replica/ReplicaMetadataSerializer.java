package com.slack.kaldb.metadata.replica;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;

public class ReplicaMetadataSerializer implements MetadataSerializer<ReplicaMetadata> {
  private static ReplicaMetadata fromReplicaMetadataProto(
      Metadata.ReplicaMetadata replicaMetadataProto) {
    return new ReplicaMetadata(
        replicaMetadataProto.getName(), replicaMetadataProto.getSnapshotId());
  }

  private static Metadata.ReplicaMetadata toReplicaMetadataProto(ReplicaMetadata metadata) {
    return Metadata.ReplicaMetadata.newBuilder()
        .setName(metadata.name)
        .setSnapshotId(metadata.snapshotId)
        .build();
  }

  @Override
  public String toJsonStr(ReplicaMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toReplicaMetadataProto(metadata));
  }

  @Override
  public ReplicaMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.ReplicaMetadata.Builder replicaMetadataBuilder = Metadata.ReplicaMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, replicaMetadataBuilder);
    return fromReplicaMetadataProto(replicaMetadataBuilder.build());
  }
}
