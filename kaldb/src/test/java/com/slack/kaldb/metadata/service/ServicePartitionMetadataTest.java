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

    final ServicePartitionMetadata servicePartitionMetadata =
        new ServicePartitionMetadata(start.toEpochMilli(), end.toEpochMilli(), list);

    assertThat(servicePartitionMetadata.startTimeEpochMs).isEqualTo(start.toEpochMilli());
    assertThat(servicePartitionMetadata.endTimeEpochMs).isEqualTo(end.toEpochMilli());
    assertThat(servicePartitionMetadata.getPartitions()).isEqualTo(list);
  }

  @Test
  public void testEqualsAndHashCode() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final List<String> list = List.of(name);

    final ServicePartitionMetadata servicePartitionMetadata1 =
        new ServicePartitionMetadata(start.toEpochMilli(), end.toEpochMilli(), list);
    final ServicePartitionMetadata servicePartitionMetadata2 =
        new ServicePartitionMetadata(start.toEpochMilli() + 2, end.toEpochMilli(), list);
    final ServicePartitionMetadata servicePartitionMetadata3 =
        new ServicePartitionMetadata(start.toEpochMilli(), end.toEpochMilli() + 3, list);
    final ServicePartitionMetadata servicePartitionMetadata4 =
        new ServicePartitionMetadata(
            start.toEpochMilli(), end.toEpochMilli(), Collections.emptyList());

    assertThat(servicePartitionMetadata1).isEqualTo(servicePartitionMetadata1);
    assertThat(servicePartitionMetadata1).isNotEqualTo(servicePartitionMetadata2);
    assertThat(servicePartitionMetadata1).isNotEqualTo(servicePartitionMetadata3);
    assertThat(servicePartitionMetadata1).isNotEqualTo(servicePartitionMetadata4);

    Set<ServicePartitionMetadata> set = new HashSet<>();
    set.add(servicePartitionMetadata1);
    set.add(servicePartitionMetadata2);
    set.add(servicePartitionMetadata3);
    set.add(servicePartitionMetadata4);
    assertThat(set.size()).isEqualTo(4);
    assertThat(set)
        .containsOnly(
            servicePartitionMetadata1,
            servicePartitionMetadata2,
            servicePartitionMetadata3,
            servicePartitionMetadata4);
  }

  @Test
  public void testValidServicePartitionMetadata() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final List<String> list = List.of(name);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ServicePartitionMetadata(0, end.toEpochMilli(), list));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ServicePartitionMetadata(start.toEpochMilli(), 0, list));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new ServicePartitionMetadata(start.toEpochMilli(), end.toEpochMilli(), null));
  }
}
