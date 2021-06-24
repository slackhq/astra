package com.slack.kaldb.metadata.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;

public class SnapshotMetadataSerializer implements MetadataSerializer<SnapshotMetadata> {
  public static Metadata.SnapshotMetadata toSnapshotMetadataProto(
      SnapshotMetadata snapshotMetadata) {
    return Metadata.SnapshotMetadata.newBuilder()
        .setName(snapshotMetadata.name)
        .setSnapshotId(snapshotMetadata.snapshotId)
        .setSnapshotPath(snapshotMetadata.snapshotPath)
        .setStartTimeUtc(snapshotMetadata.startTimeUtc)
        .setEndTimeUtc(snapshotMetadata.endTimeUtc)
        .setPartitionId(snapshotMetadata.partitionId)
        .setMaxOffset(snapshotMetadata.maxOffset)
        .build();
  }

  public static SnapshotMetadata fromSnapshotMetadataProto(
      Metadata.SnapshotMetadata protoSnapshotMetadata) {
    return new SnapshotMetadata(
        protoSnapshotMetadata.getName(),
        protoSnapshotMetadata.getSnapshotPath(),
        protoSnapshotMetadata.getSnapshotId(),
        protoSnapshotMetadata.getStartTimeUtc(),
        protoSnapshotMetadata.getEndTimeUtc(),
        protoSnapshotMetadata.getMaxOffset(),
        protoSnapshotMetadata.getPartitionId());
  }

  @Override
  public String toJsonStr(SnapshotMetadata metadata) throws InvalidProtocolBufferException {
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
