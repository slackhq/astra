package com.slack.kaldb.metadata.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.Test;

public class ServicePartitionMetadataSerializerTest {

  @Test
  public void testServicePartitionMetadata() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final long throughput = 2000;

    final ServicePartitionMetadata servicePartitionMetadata =
        new ServicePartitionMetadata(name, throughput, start.toEpochMilli(), end.toEpochMilli());

    Metadata.ServicePartitionMetadata servicePartitionMetadataProto =
        ServicePartitionMetadataSerializer.toServicePartitionMetadataProto(
            servicePartitionMetadata);

    assertThat(servicePartitionMetadataProto.getName()).isEqualTo(name);
    assertThat(servicePartitionMetadataProto.getThroughputBytes()).isEqualTo(throughput);
    assertThat(servicePartitionMetadataProto.getStartTimeEpochMs()).isEqualTo(start.toEpochMilli());
    assertThat(servicePartitionMetadataProto.getEndTimeEpochMs()).isEqualTo(end.toEpochMilli());

    ServicePartitionMetadata servicePartitionMetadataFromProto =
        ServicePartitionMetadataSerializer.fromServicePartitionMetadataProto(
            servicePartitionMetadataProto);

    assertThat(servicePartitionMetadataFromProto.name).isEqualTo(name);
    assertThat(servicePartitionMetadataFromProto.throughputBytes).isEqualTo(throughput);
    assertThat(servicePartitionMetadataFromProto.startTimeEpochMs).isEqualTo(start.toEpochMilli());
    assertThat(servicePartitionMetadataFromProto.endTimeEpochMs).isEqualTo(end.toEpochMilli());
  }
}
