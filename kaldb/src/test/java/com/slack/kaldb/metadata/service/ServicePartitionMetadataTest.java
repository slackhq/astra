package com.slack.kaldb.metadata.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class ServicePartitionMetadataTest {

  @Test
  public void testServicePartitionMetadata() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final List<String> list = List.of(name);

    final DatasetPartitionMetadata datasetPartitionMetadata =
        new DatasetPartitionMetadata(start.toEpochMilli(), end.toEpochMilli(), list);

    assertThat(datasetPartitionMetadata.startTimeEpochMs).isEqualTo(start.toEpochMilli());
    assertThat(datasetPartitionMetadata.endTimeEpochMs).isEqualTo(end.toEpochMilli());
    assertThat(datasetPartitionMetadata.getPartitions()).isEqualTo(list);
  }

  @Test
  public void testEqualsAndHashCode() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final List<String> list = List.of(name);

    final DatasetPartitionMetadata datasetPartitionMetadata1 =
        new DatasetPartitionMetadata(start.toEpochMilli(), end.toEpochMilli(), list);
    final DatasetPartitionMetadata datasetPartitionMetadata2 =
        new DatasetPartitionMetadata(start.toEpochMilli() + 2, end.toEpochMilli(), list);
    final DatasetPartitionMetadata datasetPartitionMetadata3 =
        new DatasetPartitionMetadata(start.toEpochMilli(), end.toEpochMilli() + 3, list);
    final DatasetPartitionMetadata datasetPartitionMetadata4 =
        new DatasetPartitionMetadata(
            start.toEpochMilli(), end.toEpochMilli(), Collections.emptyList());

    assertThat(datasetPartitionMetadata1).isEqualTo(datasetPartitionMetadata1);
    assertThat(datasetPartitionMetadata1).isNotEqualTo(datasetPartitionMetadata2);
    assertThat(datasetPartitionMetadata1).isNotEqualTo(datasetPartitionMetadata3);
    assertThat(datasetPartitionMetadata1).isNotEqualTo(datasetPartitionMetadata4);

    Set<DatasetPartitionMetadata> set = new HashSet<>();
    set.add(datasetPartitionMetadata1);
    set.add(datasetPartitionMetadata2);
    set.add(datasetPartitionMetadata3);
    set.add(datasetPartitionMetadata4);
    assertThat(set.size()).isEqualTo(4);
    assertThat(set)
        .containsOnly(
            datasetPartitionMetadata1,
            datasetPartitionMetadata2,
            datasetPartitionMetadata3,
            datasetPartitionMetadata4);
  }

  @Test
  public void testValidServicePartitionMetadata() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final List<String> list = List.of(name);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetPartitionMetadata(0, end.toEpochMilli(), list));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetPartitionMetadata(start.toEpochMilli(), 0, list));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new DatasetPartitionMetadata(start.toEpochMilli(), end.toEpochMilli(), null));
  }
}
