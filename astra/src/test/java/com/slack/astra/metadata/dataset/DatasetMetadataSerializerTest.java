package com.slack.astra.metadata.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.metadata.Metadata;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class DatasetMetadataSerializerTest {
  private final DatasetMetadataSerializer serDe = new DatasetMetadataSerializer();

  @Test
  public void testDatasetMetadataSerializer() throws InvalidProtocolBufferException {
    final Instant partitionStart = Instant.now();
    final Instant partitionEnd = Instant.now().plus(1, ChronoUnit.DAYS);
    final String partitionName = "partitionName";
    final List<String> partitionList = List.of(partitionName);

    final String name = "testDataset";
    final String owner = "testOwner";
    final String serviceNamePattern = "service1";
    final long throughput = 2000;
    final List<DatasetPartitionMetadata> list =
        Collections.singletonList(
            new DatasetPartitionMetadata(
                partitionStart.toEpochMilli(), partitionEnd.toEpochMilli(), partitionList));
    final DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughput, list, serviceNamePattern);

    String serializedDatasetMetadata = serDe.toJsonStr(datasetMetadata);
    assertThat(serializedDatasetMetadata).isNotEmpty();

    DatasetMetadata deserializedDatasetMetadata = serDe.fromJsonStr(serializedDatasetMetadata);
    assertThat(deserializedDatasetMetadata).isEqualTo(datasetMetadata);

    assertThat(deserializedDatasetMetadata.name).isEqualTo(name);
    assertThat(deserializedDatasetMetadata.serviceNamePattern).isEqualTo(serviceNamePattern);
    assertThat(deserializedDatasetMetadata.partitionConfigs).isEqualTo(list);
  }

  @Test
  public void testDatasetMetadataSerializerWithServiceNames()
      throws InvalidProtocolBufferException {
    final Instant partitionStart = Instant.now();
    final Instant partitionEnd = Instant.now().plus(1, ChronoUnit.DAYS);
    final String partitionName = "partitionName";
    final List<String> partitionList = List.of(partitionName);

    final String name = "testDataset";
    final String owner = "testOwner";
    final String serviceNamePattern1 = "abc";
    final long throughput = 2000;
    final List<DatasetPartitionMetadata> list =
        Collections.singletonList(
            new DatasetPartitionMetadata(
                partitionStart.toEpochMilli(), partitionEnd.toEpochMilli(), partitionList));
    final DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughput, list, serviceNamePattern1);

    String serializedDatasetMetadata = serDe.toJsonStr(datasetMetadata);
    assertThat(serializedDatasetMetadata).isNotEmpty();

    DatasetMetadata deserializedDatasetMetadata = serDe.fromJsonStr(serializedDatasetMetadata);
    assertThat(deserializedDatasetMetadata).isEqualTo(datasetMetadata);

    assertThat(deserializedDatasetMetadata.name).isEqualTo(name);
    assertThat(deserializedDatasetMetadata.serviceNamePattern).isEqualTo(serviceNamePattern1);
    assertThat(deserializedDatasetMetadata.partitionConfigs).isEqualTo(list);
  }

  @Test
  public void testDatasetMetadataSerializerDuplicatePartitions()
      throws InvalidProtocolBufferException {
    final Instant partitionStart1 = Instant.now();
    final Instant partitionEnd1 = Instant.now().plus(1, ChronoUnit.DAYS);
    final Instant partitionStart2 = partitionEnd1.plus(1, ChronoUnit.MILLIS);
    final Instant partitionEnd2 = partitionStart2.plus(1, ChronoUnit.DAYS);
    final String partitionName = "partitionName1";
    final List<String> partitionList = List.of(partitionName);

    final String name = "testDataset";
    final String owner = "testOwner";
    final String serviceNamePattern = "serviceName";
    final long throughput = 2000;
    final List<DatasetPartitionMetadata> list =
        List.of(
            new DatasetPartitionMetadata(
                partitionStart1.toEpochMilli(), partitionEnd1.toEpochMilli(), partitionList),
            new DatasetPartitionMetadata(
                partitionStart2.toEpochMilli(), partitionEnd2.toEpochMilli(), partitionList));
    final DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughput, list, serviceNamePattern);

    String serializedDatasetMetadata = serDe.toJsonStr(datasetMetadata);
    assertThat(serializedDatasetMetadata).isNotEmpty();

    DatasetMetadata deserializedDatasetMetadata = serDe.fromJsonStr(serializedDatasetMetadata);
    assertThat(deserializedDatasetMetadata).isEqualTo(datasetMetadata);

    assertThat(deserializedDatasetMetadata.name).isEqualTo(name);
    assertThat(deserializedDatasetMetadata.serviceNamePattern).isEqualTo(serviceNamePattern);
    assertThat(deserializedDatasetMetadata.partitionConfigs).isEqualTo(list);
  }

  @Test
  public void testDatasetMetadataSerializerEmptyList() throws InvalidProtocolBufferException {
    final String name = "testDataset";
    final String owner = "testOwner";
    final String serviceNamePattern = "serviceName";
    final long throughput = 2000;
    final List<DatasetPartitionMetadata> list = Collections.emptyList();
    final DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughput, list, serviceNamePattern);

    String serializedDatasetMetadata = serDe.toJsonStr(datasetMetadata);
    assertThat(serializedDatasetMetadata).isNotEmpty();

    DatasetMetadata deserializedDatasetMetadata = serDe.fromJsonStr(serializedDatasetMetadata);
    assertThat(deserializedDatasetMetadata).isEqualTo(datasetMetadata);

    assertThat(deserializedDatasetMetadata.name).isEqualTo(name);
    assertThat(deserializedDatasetMetadata.serviceNamePattern).isEqualTo(serviceNamePattern);
    assertThat(deserializedDatasetMetadata.partitionConfigs).isEqualTo(list);
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
  public void testDatasetPartitionMetadata() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final List<String> list = List.of(name);

    final DatasetPartitionMetadata datasetPartitionMetadata =
        new DatasetPartitionMetadata(start.toEpochMilli(), end.toEpochMilli(), list);

    Metadata.DatasetPartitionMetadata datasetPartitionMetadataProto =
        DatasetPartitionMetadata.toDatasetPartitionMetadataProto(datasetPartitionMetadata);

    assertThat(datasetPartitionMetadataProto.getStartTimeEpochMs()).isEqualTo(start.toEpochMilli());
    assertThat(datasetPartitionMetadataProto.getEndTimeEpochMs()).isEqualTo(end.toEpochMilli());
    assertThat(datasetPartitionMetadataProto.getPartitionsList()).isEqualTo(list);

    DatasetPartitionMetadata datasetPartitionMetadataFromProto =
        DatasetPartitionMetadata.fromDatasetPartitionMetadataProto(datasetPartitionMetadataProto);

    assertThat(datasetPartitionMetadataFromProto.startTimeEpochMs).isEqualTo(start.toEpochMilli());
    assertThat(datasetPartitionMetadataFromProto.endTimeEpochMs).isEqualTo(end.toEpochMilli());
    assertThat(datasetPartitionMetadataFromProto.getPartitions()).isEqualTo(list);
  }
}
