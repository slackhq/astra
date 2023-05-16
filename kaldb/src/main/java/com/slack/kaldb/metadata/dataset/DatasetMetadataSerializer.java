package com.slack.kaldb.metadata.dataset;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;
import java.util.List;

public class DatasetMetadataSerializer implements MetadataSerializer<DatasetMetadata> {
  private static DatasetMetadata fromDatasetMetadataProto(
      Metadata.DatasetMetadata datasetMetadataProto) {
    List<DatasetPartitionMetadata> datasetPartitionMetadata =
        datasetMetadataProto
            .getPartitionConfigsList()
            .stream()
            .map(DatasetPartitionMetadata::fromDatasetPartitionMetadataProto)
            .toList();

    return new DatasetMetadata(
        datasetMetadataProto.getName(),
        datasetMetadataProto.getOwner(),
        datasetMetadataProto.getThroughputBytes(),
        datasetPartitionMetadata,
        datasetMetadataProto.getServiceNamePattern());
  }

  public static Metadata.DatasetMetadata toDatasetMetadataProto(DatasetMetadata metadata) {
    List<Metadata.DatasetPartitionMetadata> datasetPartitionMetadata =
        metadata
            .partitionConfigs
            .stream()
            .map(DatasetPartitionMetadata::toDatasetPartitionMetadataProto)
            .toList();

    return Metadata.DatasetMetadata.newBuilder()
        .setName(metadata.name)
        .setOwner(metadata.owner)
        .setThroughputBytes(metadata.throughputBytes)
        .addAllPartitionConfigs(datasetPartitionMetadata)
        .setServiceNamePattern(metadata.serviceNamePattern)
        .build();
  }

  @Override
  public String toJsonStr(DatasetMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toDatasetMetadataProto(metadata));
  }

  @Override
  public DatasetMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.DatasetMetadata.Builder datasetMetadataBuilder = Metadata.DatasetMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, datasetMetadataBuilder);
    return fromDatasetMetadataProto(datasetMetadataBuilder.build());
  }
}
