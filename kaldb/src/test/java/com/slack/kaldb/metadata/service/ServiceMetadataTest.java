package com.slack.kaldb.metadata.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class ServiceMetadataTest {

  @Test
  public void testServiceMetadata() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final ServicePartitionMetadata partition =
        new ServicePartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("partition"));
    final List<ServicePartitionMetadata> partitionConfigs = Collections.singletonList(partition);
    ServiceMetadata serviceMetadata =
        new ServiceMetadata(name, owner, throughputBytes, partitionConfigs);

    assertThat(serviceMetadata.name).isEqualTo(name);
    assertThat(serviceMetadata.owner).isEqualTo(owner);
    assertThat(serviceMetadata.throughputBytes).isEqualTo(throughputBytes);
    assertThat(serviceMetadata.partitionConfigs).isEqualTo(partitionConfigs);
  }

  @Test
  public void testServiceMetadataImmutableList() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final ServicePartitionMetadata partition =
        new ServicePartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("partition"));
    final List<ServicePartitionMetadata> partitionConfigs1 = Collections.singletonList(partition);
    ServiceMetadata serviceMetadata =
        new ServiceMetadata(name, owner, throughputBytes, partitionConfigs1);

    assertThat(serviceMetadata.name).isEqualTo(name);
    assertThat(serviceMetadata.owner).isEqualTo(owner);
    assertThat(serviceMetadata.throughputBytes).isEqualTo(throughputBytes);
    assertThat(serviceMetadata.partitionConfigs).isEqualTo(partitionConfigs1);

    final ServicePartitionMetadata partition2 =
        new ServicePartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("partition2"));

    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(() -> serviceMetadata.partitionConfigs.add(partition2));
  }

  @Test
  public void testEqualsAndHashCode() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final ServicePartitionMetadata partition =
        new ServicePartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("partition"));
    final List<ServicePartitionMetadata> partitionConfig = Collections.singletonList(partition);

    ServiceMetadata serviceMetadata1 =
        new ServiceMetadata(name, owner, throughputBytes, partitionConfig);
    ServiceMetadata serviceMetadata2 =
        new ServiceMetadata(name + "2", owner, throughputBytes, partitionConfig);
    ServiceMetadata serviceMetadata3 =
        new ServiceMetadata(name, owner + "3", throughputBytes, partitionConfig);
    ServiceMetadata serviceMetadata4 =
        new ServiceMetadata(name, owner, throughputBytes + 4, partitionConfig);
    ServiceMetadata serviceMetadata5 =
        new ServiceMetadata(name, owner, throughputBytes, Collections.emptyList());

    assertThat(serviceMetadata1).isEqualTo(serviceMetadata1);
    assertThat(serviceMetadata1).isNotEqualTo(serviceMetadata2);
    assertThat(serviceMetadata1).isNotEqualTo(serviceMetadata3);
    assertThat(serviceMetadata1).isNotEqualTo(serviceMetadata4);
    assertThat(serviceMetadata1).isNotEqualTo(serviceMetadata5);

    Set<ServiceMetadata> set = new HashSet<>();
    set.add(serviceMetadata1);
    set.add(serviceMetadata2);
    set.add(serviceMetadata3);
    set.add(serviceMetadata4);
    set.add(serviceMetadata5);
    assertThat(set.size()).isEqualTo(5);
    assertThat(set)
        .containsOnly(
            serviceMetadata1,
            serviceMetadata2,
            serviceMetadata3,
            serviceMetadata4,
            serviceMetadata5);
  }

  @Test
  public void testValidServiceMetadata() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final ServicePartitionMetadata partition =
        new ServicePartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("partition"));
    final List<ServicePartitionMetadata> partitionConfig = Collections.singletonList(partition);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ServiceMetadata("", owner, throughputBytes, partitionConfig));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ServiceMetadata(null, owner, throughputBytes, partitionConfig));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ServiceMetadata(name, "", throughputBytes, partitionConfig));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ServiceMetadata(name, null, throughputBytes, partitionConfig));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ServiceMetadata(name, owner, 0, partitionConfig));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ServiceMetadata(name, owner, throughputBytes, null));
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
                new ServiceMetadata(
                    name,
                    owner,
                    throughputBytes,
                    List.of(
                        new ServicePartitionMetadata(0, 2000, partitionlist),
                        new ServicePartitionMetadata(0, 3000, partitionlist))));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ServiceMetadata(
                    name,
                    owner,
                    throughputBytes,
                    List.of(
                        new ServicePartitionMetadata(0, 2000, partitionlist),
                        new ServicePartitionMetadata(2000, 3000, partitionlist))));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ServiceMetadata(
                    name,
                    owner,
                    throughputBytes,
                    List.of(
                        new ServicePartitionMetadata(0, 3000, partitionlist),
                        new ServicePartitionMetadata(0, 2000, partitionlist))));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ServiceMetadata(
                    name,
                    owner,
                    throughputBytes,
                    List.of(
                        new ServicePartitionMetadata(0, 2000, partitionlist),
                        new ServicePartitionMetadata(1800, 3000, partitionlist))));
  }
}
