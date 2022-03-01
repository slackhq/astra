package com.slack.kaldb.metadata.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class ServiceMetadataSerializerTest {
  private final ServiceMetadataSerializer serDe = new ServiceMetadataSerializer();

  @Test
  public void testServiceMetadataSerializer() throws InvalidProtocolBufferException {
    final Instant partitionStart = Instant.now();
    final Instant partitionEnd = Instant.now().plus(1, ChronoUnit.DAYS);
    final String partitionName = "partitionName";
    final List<String> partitionList = List.of(partitionName);

    final String name = "testService";
    final String owner = "testOwner";
    final long throughput = 2000;
    final List<ServicePartitionMetadata> list =
        Collections.singletonList(
            new ServicePartitionMetadata(
                partitionStart.toEpochMilli(), partitionEnd.toEpochMilli(), partitionList));
    final ServiceMetadata serviceMetadata = new ServiceMetadata(name, owner, throughput, list);

    String serializedServiceMetadata = serDe.toJsonStr(serviceMetadata);
    assertThat(serializedServiceMetadata).isNotEmpty();

    ServiceMetadata deserializedServiceMetadata = serDe.fromJsonStr(serializedServiceMetadata);
    assertThat(deserializedServiceMetadata).isEqualTo(serviceMetadata);

    assertThat(deserializedServiceMetadata.name).isEqualTo(name);
    assertThat(deserializedServiceMetadata.partitionConfigs).isEqualTo(list);
  }

  @Test
  public void testServiceMetadataSerializerDuplicatePartitions()
      throws InvalidProtocolBufferException {
    final Instant partitionStart1 = Instant.now();
    final Instant partitionEnd1 = Instant.now().plus(1, ChronoUnit.DAYS);
    final Instant partitionStart2 = partitionEnd1.plus(1, ChronoUnit.MILLIS);
    final Instant partitionEnd2 = partitionStart2.plus(1, ChronoUnit.DAYS);
    final String partitionName = "partitionName1";
    final List<String> partitionList = List.of(partitionName);

    final String name = "testService";
    final String owner = "testOwner";
    final long throughput = 2000;
    final List<ServicePartitionMetadata> list =
        List.of(
            new ServicePartitionMetadata(
                partitionStart1.toEpochMilli(), partitionEnd1.toEpochMilli(), partitionList),
            new ServicePartitionMetadata(
                partitionStart2.toEpochMilli(), partitionEnd2.toEpochMilli(), partitionList));
    final ServiceMetadata serviceMetadata = new ServiceMetadata(name, owner, throughput, list);

    String serializedServiceMetadata = serDe.toJsonStr(serviceMetadata);
    assertThat(serializedServiceMetadata).isNotEmpty();

    ServiceMetadata deserializedServiceMetadata = serDe.fromJsonStr(serializedServiceMetadata);
    assertThat(deserializedServiceMetadata).isEqualTo(serviceMetadata);

    assertThat(deserializedServiceMetadata.name).isEqualTo(name);
    assertThat(deserializedServiceMetadata.partitionConfigs).isEqualTo(list);
  }

  @Test
  public void testServiceMetadataSerializerEmptyList() throws InvalidProtocolBufferException {
    final String name = "testService";
    final String owner = "testOwner";
    final long throughput = 2000;
    final List<ServicePartitionMetadata> list = Collections.emptyList();
    final ServiceMetadata serviceMetadata = new ServiceMetadata(name, owner, throughput, list);

    String serializedServiceMetadata = serDe.toJsonStr(serviceMetadata);
    assertThat(serializedServiceMetadata).isNotEmpty();

    ServiceMetadata deserializedServiceMetadata = serDe.fromJsonStr(serializedServiceMetadata);
    assertThat(deserializedServiceMetadata).isEqualTo(serviceMetadata);

    assertThat(deserializedServiceMetadata.name).isEqualTo(name);
    assertThat(deserializedServiceMetadata.partitionConfigs).isEqualTo(list);
  }

  @Test
  public void testInvalidSerializations() {
    Throwable serializeNull = catchThrowable(() -> serDe.toJsonStr(null));
    assertThat(serializeNull).isInstanceOf(IllegalArgumentException.class);

    Throwable deserializeNull = catchThrowable(() -> serDe.fromJsonStr(null));
    assertThat(deserializeNull).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeEmpty = catchThrowable(() -> serDe.fromJsonStr(""));
    assertThat(deserializeEmpty).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeCorrupt = catchThrowable(() -> serDe.fromJsonStr("test"));
    assertThat(deserializeCorrupt).isInstanceOf(InvalidProtocolBufferException.class);
  }

  @Test
  public void testServicePartitionMetadata() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final List<String> list = List.of(name);

    final ServicePartitionMetadata servicePartitionMetadata =
        new ServicePartitionMetadata(start.toEpochMilli(), end.toEpochMilli(), list);

    Metadata.ServicePartitionMetadata servicePartitionMetadataProto =
        ServicePartitionMetadata.toServicePartitionMetadataProto(servicePartitionMetadata);

    assertThat(servicePartitionMetadataProto.getStartTimeEpochMs()).isEqualTo(start.toEpochMilli());
    assertThat(servicePartitionMetadataProto.getEndTimeEpochMs()).isEqualTo(end.toEpochMilli());
    assertThat(servicePartitionMetadataProto.getPartitionsList()).isEqualTo(list);

    ServicePartitionMetadata servicePartitionMetadataFromProto =
        ServicePartitionMetadata.fromServicePartitionMetadataProto(servicePartitionMetadataProto);

    assertThat(servicePartitionMetadataFromProto.startTimeEpochMs).isEqualTo(start.toEpochMilli());
    assertThat(servicePartitionMetadataFromProto.endTimeEpochMs).isEqualTo(end.toEpochMilli());
    assertThat(servicePartitionMetadataFromProto.getPartitions()).isEqualTo(list);
  }
}
