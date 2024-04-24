package com.slack.astra.metadata.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

public class DatasetMetadataTest {

  @Test
  public void testServiceMetadata() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition =
        new DatasetPartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("partition"));
    final List<DatasetPartitionMetadata> partitionConfigs = Collections.singletonList(partition);
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughputBytes, partitionConfigs, name);

    assertThat(datasetMetadata.name).isEqualTo(name);
    assertThat(datasetMetadata.owner).isEqualTo(owner);
    assertThat(datasetMetadata.throughputBytes).isEqualTo(throughputBytes);
    assertThat(datasetMetadata.partitionConfigs).isEqualTo(partitionConfigs);
  }

  @Test
  public void testInvalidServiceMetadataNames() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetMetadata("&", "owner", 0, null, "&"));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetMetadata("test%", "owner", 0, null, "test%"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new DatasetMetadata(
                    "jZOGhT2v86abA0h6yX6DUeKOkKE06nR0TlExO0bp7HBv"
                        + "uVPS1xwXxZAK6pSW9zjdSDYe9v8JfuJvCsiyjGyQv2v0lYc1H3ZGkYTeGjdtnPCQ0I7EIxFRr1UpGpoud7b0"
                        + "xq3sEboigNrDG8carETSj8c95Aj4EW4RudkDjRHjUhrA5aLZ688LfZVIJpnom3kgyxlAnhhrdVQsdN1J2qL7"
                        + "FY9LMMsA2aPtQ7Q8g4eZzm6Kv51r0x26pFQhxfHCwUZqa",
                    "owner",
                    0,
                    null,
                    "jZOGhT2v86abA0h6yX6DUeKOkKE06nR0TlExO0bp7HBv"));

    final DatasetPartitionMetadata partition =
        new DatasetPartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("partition"));
    final List<DatasetPartitionMetadata> partitionConfigs1 = Collections.singletonList(partition);
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new DatasetMetadata(
                    "name", "owner", 1, partitionConfigs1, RandomStringUtils.random(257)));
  }

  @Test
  @SuppressWarnings("DoNotCall")
  public void testServiceMetadataImmutableList() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition =
        new DatasetPartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("partition"));
    final List<DatasetPartitionMetadata> partitionConfigs1 = Collections.singletonList(partition);
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughputBytes, partitionConfigs1, name);

    assertThat(datasetMetadata.name).isEqualTo(name);
    assertThat(datasetMetadata.owner).isEqualTo(owner);
    assertThat(datasetMetadata.throughputBytes).isEqualTo(throughputBytes);
    assertThat(datasetMetadata.partitionConfigs).isEqualTo(partitionConfigs1);

    final DatasetPartitionMetadata partition2 =
        new DatasetPartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("partition2"));

    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(() -> datasetMetadata.partitionConfigs.add(partition2));
  }

  @Test
  public void testEqualsAndHashCode() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition =
        new DatasetPartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("partition"));
    final List<DatasetPartitionMetadata> partitionConfig = Collections.singletonList(partition);

    DatasetMetadata datasetMetadata1 =
        new DatasetMetadata(name, owner, throughputBytes, partitionConfig, name);
    DatasetMetadata datasetMetadata2 =
        new DatasetMetadata(name + "2", owner, throughputBytes, partitionConfig, name + "2");
    DatasetMetadata datasetMetadata3 =
        new DatasetMetadata(name, owner + "3", throughputBytes, partitionConfig, name);
    DatasetMetadata datasetMetadata4 =
        new DatasetMetadata(name, owner, throughputBytes + 4, partitionConfig, name);
    DatasetMetadata datasetMetadata5 =
        new DatasetMetadata(name, owner, throughputBytes, Collections.emptyList(), name);

    assertThat(datasetMetadata1).isNotEqualTo(datasetMetadata2);
    assertThat(datasetMetadata1).isNotEqualTo(datasetMetadata3);
    assertThat(datasetMetadata1).isNotEqualTo(datasetMetadata4);
    assertThat(datasetMetadata1).isNotEqualTo(datasetMetadata5);

    Set<DatasetMetadata> set = new HashSet<>();
    set.add(datasetMetadata1);
    set.add(datasetMetadata2);
    set.add(datasetMetadata3);
    set.add(datasetMetadata4);
    set.add(datasetMetadata5);
    assertThat(set.size()).isEqualTo(5);
    assertThat(set)
        .containsOnly(
            datasetMetadata1,
            datasetMetadata2,
            datasetMetadata3,
            datasetMetadata4,
            datasetMetadata5);
  }

  @Test
  public void testValidServiceMetadata() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition =
        new DatasetPartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("partition"));
    final List<DatasetPartitionMetadata> partitionConfig = Collections.singletonList(partition);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetMetadata("", owner, throughputBytes, partitionConfig, ""));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetMetadata(null, owner, throughputBytes, partitionConfig, null));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetMetadata(name, "", throughputBytes, partitionConfig, name));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetMetadata(name, null, throughputBytes, partitionConfig, name));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetMetadata(name, owner, -1, partitionConfig, name));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetMetadata(name, owner, throughputBytes, null, name));
  }

  @Test
  public void testValidServicePartitions() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final String partitionName = "partition";
    final List<String> partitionlist = List.of(partitionName);
    final long throughputBytes = 2000;

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new DatasetMetadata(
                    name,
                    owner,
                    throughputBytes,
                    List.of(
                        new DatasetPartitionMetadata(0, 2000, partitionlist),
                        new DatasetPartitionMetadata(0, 3000, partitionlist)),
                    name));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new DatasetMetadata(
                    name,
                    owner,
                    throughputBytes,
                    List.of(
                        new DatasetPartitionMetadata(0, 2000, partitionlist),
                        new DatasetPartitionMetadata(2000, 3000, partitionlist)),
                    name));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new DatasetMetadata(
                    name,
                    owner,
                    throughputBytes,
                    List.of(
                        new DatasetPartitionMetadata(0, 3000, partitionlist),
                        new DatasetPartitionMetadata(0, 2000, partitionlist)),
                    name));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new DatasetMetadata(
                    name,
                    owner,
                    throughputBytes,
                    List.of(
                        new DatasetPartitionMetadata(0, 2000, partitionlist),
                        new DatasetPartitionMetadata(1800, 3000, partitionlist)),
                    name));
  }
}
