package com.slack.kaldb.metadata.dataset;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;
import java.util.List;
import java.util.stream.Collectors;

public class DatasetMetadataSerializer implements MetadataSerializer<DatasetMetadata> {
  private static DatasetMetadata fromServiceMetadataProto(
      Metadata.DatasetMetadata datasetMetadataProto) {
    List<DatasetPartitionMetadata> datasetPartitionMetadata =
        datasetMetadataProto
            .getPartitionConfigsList()
            .stream()
            .map(DatasetPartitionMetadata::fromServicePartitionMetadataProto)
            .collect(Collectors.toList());

    return new DatasetMetadata(
        datasetMetadataProto.getName(),
        datasetMetadataProto.getOwner(),
        datasetMetadataProto.getThroughputBytes(),
        datasetPartitionMetadata);
  }

  public static Metadata.DatasetMetadata toServiceMetadataProto(DatasetMetadata metadata) {
    List<Metadata.DatasetPartitionMetadata> datasetPartitionMetadata =
        metadata
            .partitionConfigs
            .stream()
            .map(DatasetPartitionMetadata::toServicePartitionMetadataProto)
            .collect(Collectors.toList());

    return Metadata.DatasetMetadata.newBuilder()
        .setName(metadata.name)
        .setOwner(metadata.owner)
        .setThroughputBytes(metadata.throughputBytes)
        .addAllPartitionConfigs(datasetPartitionMetadata)
        .build();
  }

  @Override
  public String toJsonStr(DatasetMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toServiceMetadataProto(metadata));
  }

  @Override
  public DatasetMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.DatasetMetadata.Builder serviceMetadataBuilder = Metadata.DatasetMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, serviceMetadataBuilder);
    return fromServiceMetadataProto(serviceMetadataBuilder.build());
  }
}
