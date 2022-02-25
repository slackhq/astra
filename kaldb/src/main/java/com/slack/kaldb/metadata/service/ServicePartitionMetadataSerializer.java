package com.slack.kaldb.metadata.service;

import com.slack.kaldb.proto.metadata.Metadata;

public class ServicePartitionMetadataSerializer {

  public static ServicePartitionMetadata fromServicePartitionMetadataProto(
      Metadata.ServicePartitionMetadata servicePartitionMetadata) {
    return new ServicePartitionMetadata(
        servicePartitionMetadata.getName(),
        servicePartitionMetadata.getThroughputBytes(),
        servicePartitionMetadata.getStartTimeEpochMs(),
        servicePartitionMetadata.getEndTimeEpochMs());
  }

  public static Metadata.ServicePartitionMetadata toServicePartitionMetadataProto(
      ServicePartitionMetadata metadata) {
    return Metadata.ServicePartitionMetadata.newBuilder()
        .setName(metadata.name)
        .setThroughputBytes(metadata.throughputBytes)
        .setStartTimeEpochMs(metadata.startTimeEpochMs)
        .setEndTimeEpochMs(metadata.endTimeEpochMs)
        .build();
  }
}
