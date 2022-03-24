package com.slack.kaldb.metadata.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;
import java.util.List;
import java.util.stream.Collectors;

public class ServiceMetadataSerializer implements MetadataSerializer<ServiceMetadata> {
  private static ServiceMetadata fromServiceMetadataProto(
      Metadata.ServiceMetadata serviceMetadataProto) {
    List<ServicePartitionMetadata> servicePartitionMetadata =
        serviceMetadataProto
            .getPartitionConfigsList()
            .stream()
            .map(ServicePartitionMetadata::fromServicePartitionMetadataProto)
            .collect(Collectors.toList());

    return new ServiceMetadata(
        serviceMetadataProto.getName(),
        serviceMetadataProto.getOwner(),
        serviceMetadataProto.getThroughputBytes(),
        servicePartitionMetadata);
  }

  public static Metadata.ServiceMetadata toServiceMetadataProto(ServiceMetadata metadata) {
    List<Metadata.ServicePartitionMetadata> servicePartitionMetadata =
        metadata
            .partitionConfigs
            .stream()
            .map(ServicePartitionMetadata::toServicePartitionMetadataProto)
            .collect(Collectors.toList());

    return Metadata.ServiceMetadata.newBuilder()
        .setName(metadata.name)
        .setOwner(metadata.owner)
        .setThroughputBytes(metadata.throughputBytes)
        .addAllPartitionConfigs(servicePartitionMetadata)
        .build();
  }

  @Override
  public String toJsonStr(ServiceMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toServiceMetadataProto(metadata));
  }

  @Override
  public ServiceMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.ServiceMetadata.Builder serviceMetadataBuilder = Metadata.ServiceMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, serviceMetadataBuilder);
    return fromServiceMetadataProto(serviceMetadataBuilder.build());
  }
}
