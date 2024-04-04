package com.slack.astra.metadata.recovery;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class RecoveryNodeMetadataSerializer implements MetadataSerializer<RecoveryNodeMetadata> {

  private static RecoveryNodeMetadata fromRecoveryNodeMetadataProto(
      Metadata.RecoveryNodeMetadata recoveryNodeMetadataProto) {
    return new RecoveryNodeMetadata(
        recoveryNodeMetadataProto.getName(),
        recoveryNodeMetadataProto.getRecoveryNodeState(),
        recoveryNodeMetadataProto.getRecoveryTaskName(),
        recoveryNodeMetadataProto.getUpdatedTimeEpochMs());
  }

  private static Metadata.RecoveryNodeMetadata toRecoveryNodeMetadataProto(
      RecoveryNodeMetadata metadata) {
    return Metadata.RecoveryNodeMetadata.newBuilder()
        .setName(metadata.name)
        .setRecoveryNodeState(metadata.recoveryNodeState)
        .setRecoveryTaskName(metadata.recoveryTaskName)
        .setUpdatedTimeEpochMs(metadata.updatedTimeEpochMs)
        .build();
  }

  @Override
  public String toJsonStr(RecoveryNodeMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toRecoveryNodeMetadataProto(metadata));
  }

  @Override
  public RecoveryNodeMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.RecoveryNodeMetadata.Builder recoveryNodeMetadataBuilder =
        Metadata.RecoveryNodeMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, recoveryNodeMetadataBuilder);
    return fromRecoveryNodeMetadataProto(recoveryNodeMetadataBuilder.build());
  }
}
