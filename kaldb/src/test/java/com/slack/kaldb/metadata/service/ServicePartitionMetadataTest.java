package com.slack.kaldb.metadata.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class ServicePartitionMetadataTest {

  @Test
  public void testServicePartitionMetadata() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final long throughput = 2000;

    final ServicePartitionMetadata servicePartitionMetadata =
        new ServicePartitionMetadata(name, throughput, start.toEpochMilli(), end.toEpochMilli());

    assertThat(servicePartitionMetadata.name).isEqualTo(name);
    assertThat(servicePartitionMetadata.startTimeEpochMs).isEqualTo(start.toEpochMilli());
    assertThat(servicePartitionMetadata.endTimeEpochMs).isEqualTo(end.toEpochMilli());
    assertThat(servicePartitionMetadata.throughputBytes).isEqualTo(throughput);
  }

  @Test
  public void testEqualsAndHashCode() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final long throughput = 2000;

    final ServicePartitionMetadata servicePartitionMetadata1 =
        new ServicePartitionMetadata(name, throughput, start.toEpochMilli(), end.toEpochMilli());
    final ServicePartitionMetadata servicePartitionMetadata2 =
        new ServicePartitionMetadata(
            name + "2", throughput, start.toEpochMilli(), end.toEpochMilli());
    final ServicePartitionMetadata servicePartitionMetadata3 =
        new ServicePartitionMetadata(
            name, throughput + 3, start.toEpochMilli(), end.toEpochMilli());
    final ServicePartitionMetadata servicePartitionMetadata4 =
        new ServicePartitionMetadata(
            name, throughput, start.toEpochMilli() + 4, end.toEpochMilli());
    final ServicePartitionMetadata servicePartitionMetadata5 =
        new ServicePartitionMetadata(
            name, throughput, start.toEpochMilli(), end.toEpochMilli() + 5);

    assertThat(servicePartitionMetadata1).isEqualTo(servicePartitionMetadata1);
    assertThat(servicePartitionMetadata1).isNotEqualTo(servicePartitionMetadata2);
    assertThat(servicePartitionMetadata1).isNotEqualTo(servicePartitionMetadata3);
    assertThat(servicePartitionMetadata1).isNotEqualTo(servicePartitionMetadata4);
    assertThat(servicePartitionMetadata1).isNotEqualTo(servicePartitionMetadata5);

    Set<ServicePartitionMetadata> set = new HashSet<>();
    set.add(servicePartitionMetadata1);
    set.add(servicePartitionMetadata2);
    set.add(servicePartitionMetadata3);
    set.add(servicePartitionMetadata4);
    set.add(servicePartitionMetadata5);
    assertThat(set.size()).isEqualTo(5);
    assertThat(set)
        .containsOnly(
            servicePartitionMetadata1,
            servicePartitionMetadata2,
            servicePartitionMetadata3,
            servicePartitionMetadata4,
            servicePartitionMetadata5);
  }

  @Test
  public void testValidServicePartitionMetadata() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final long throughput = 2000;

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ServicePartitionMetadata(
                    "", throughput, start.toEpochMilli(), end.toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ServicePartitionMetadata(
                    null, throughput, start.toEpochMilli(), end.toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new ServicePartitionMetadata(name, 0, start.toEpochMilli(), end.toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ServicePartitionMetadata(name, throughput, 0, end.toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ServicePartitionMetadata(
                    name, throughput, start.toEpochMilli(), start.toEpochMilli()));
  }
}
