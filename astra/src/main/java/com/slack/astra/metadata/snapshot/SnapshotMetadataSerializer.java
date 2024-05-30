package com.slack.astra.metadata.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class SnapshotMetadataSerializer implements MetadataSerializer<SnapshotMetadata> {
  private static Metadata.SnapshotMetadata toSnapshotMetadataProto(
      SnapshotMetadata snapshotMetadata) {
    return Metadata.SnapshotMetadata.newBuilder()
        .setName(snapshotMetadata.name)
        .setSnapshotId(snapshotMetadata.snapshotId)
        .setSnapshotPath(snapshotMetadata.snapshotPath)
        .setStartTimeEpochMs(snapshotMetadata.startTimeEpochMs)
        .setEndTimeEpochMs(snapshotMetadata.endTimeEpochMs)
        .setPartitionId(snapshotMetadata.partitionId)
        .setMaxOffset(snapshotMetadata.maxOffset)
        .setIndexType(snapshotMetadata.indexType)
        .setSizeInBytes(snapshotMetadata.sizeInBytes)
        .build();
  }

  private static SnapshotMetadata fromSnapshotMetadataProto(
      Metadata.SnapshotMetadata protoSnapshotMetadata) {
    return new SnapshotMetadata(
        protoSnapshotMetadata.getSnapshotId(),
        protoSnapshotMetadata.getSnapshotPath(),
        protoSnapshotMetadata.getStartTimeEpochMs(),
        protoSnapshotMetadata.getEndTimeEpochMs(),
        protoSnapshotMetadata.getMaxOffset(),
        protoSnapshotMetadata.getPartitionId(),
        Metadata.IndexType.LOGS_LUCENE9,
        protoSnapshotMetadata.getSizeInBytes());
  }

  @Override
  public String toJsonStr(SnapshotMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toSnapshotMetadataProto(metadata));
  }

  @Override
  public SnapshotMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.SnapshotMetadata.Builder snapshotMetadataBuiler =
        Metadata.SnapshotMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, snapshotMetadataBuiler);
    return fromSnapshotMetadataProto(snapshotMetadataBuiler.build());
  }
}
