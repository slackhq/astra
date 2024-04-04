package com.slack.astra.metadata.recovery;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class RecoveryTaskMetadataSerializer implements MetadataSerializer<RecoveryTaskMetadata> {

  private static RecoveryTaskMetadata fromRecoveryTaskMetadataProto(
      Metadata.RecoveryTaskMetadata recoveryTaskMetadataProto) {
    return new RecoveryTaskMetadata(
        recoveryTaskMetadataProto.getName(),
        recoveryTaskMetadataProto.getPartitionId(),
        recoveryTaskMetadataProto.getStartOffset(),
        recoveryTaskMetadataProto.getEndOffset(),
        recoveryTaskMetadataProto.getCreatedTimeEpochMs());
  }

  private static Metadata.RecoveryTaskMetadata toRecoveryTaskMetadataProto(
      RecoveryTaskMetadata metadata) {
    return Metadata.RecoveryTaskMetadata.newBuilder()
        .setName(metadata.name)
        .setPartitionId(metadata.partitionId)
        .setStartOffset(metadata.startOffset)
        .setEndOffset(metadata.endOffset)
        .setCreatedTimeEpochMs(metadata.createdTimeEpochMs)
        .build();
  }

  @Override
  public String toJsonStr(RecoveryTaskMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toRecoveryTaskMetadataProto(metadata));
  }

  @Override
  public RecoveryTaskMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.RecoveryTaskMetadata.Builder recoveryTaskMetadataBuilder =
        Metadata.RecoveryTaskMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, recoveryTaskMetadataBuilder);
    return fromRecoveryTaskMetadataProto(recoveryTaskMetadataBuilder.build());
  }
}
