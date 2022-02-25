package com.slack.kaldb.metadata.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
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
    final long partitionThroughput = 2000;

    final String name = "testService";
    final List<ServicePartitionMetadata> list =
        Collections.singletonList(
            new ServicePartitionMetadata(
                partitionName,
                partitionThroughput,
                partitionStart.toEpochMilli(),
                partitionEnd.toEpochMilli()));
    final ServiceMetadata serviceMetadata = new ServiceMetadata(name, list);

    String serializedServiceMetadata = serDe.toJsonStr(serviceMetadata);
    assertThat(serializedServiceMetadata).isNotEmpty();

    ServiceMetadata deserializedServiceMetadata = serDe.fromJsonStr(serializedServiceMetadata);
    assertThat(deserializedServiceMetadata).isEqualTo(serviceMetadata);

    assertThat(deserializedServiceMetadata.name).isEqualTo(name);
    assertThat(deserializedServiceMetadata.partitionList).isEqualTo(list);
  }

  @Test
  public void testServiceMetadataSerializerEmptyList() throws InvalidProtocolBufferException {
    final String name = "testService";
    final List<ServicePartitionMetadata> list = Collections.emptyList();
    final ServiceMetadata serviceMetadata = new ServiceMetadata(name, list);

    String serializedServiceMetadata = serDe.toJsonStr(serviceMetadata);
    assertThat(serializedServiceMetadata).isNotEmpty();

    ServiceMetadata deserializedServiceMetadata = serDe.fromJsonStr(serializedServiceMetadata);
    assertThat(deserializedServiceMetadata).isEqualTo(serviceMetadata);

    assertThat(deserializedServiceMetadata.name).isEqualTo(name);
    assertThat(deserializedServiceMetadata.partitionList).isEqualTo(list);
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
}
