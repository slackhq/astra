package com.slack.kaldb.metadata.service;

import static org.assertj.core.api.Assertions.assertThat;
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
    final ServicePartitionMetadata partition =
        new ServicePartitionMetadata(
            "partition",
            1000,
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli());
    final List<ServicePartitionMetadata> partitionList = Collections.singletonList(partition);
    ServiceMetadata serviceMetadata = new ServiceMetadata(name, partitionList);

    assertThat(serviceMetadata.name).isEqualTo(name);
    assertThat(serviceMetadata.partitionList).isEqualTo(partitionList);
  }

  @Test
  public void testEqualsAndHashCode() {
    final String name = "testService";
    final ServicePartitionMetadata partition =
        new ServicePartitionMetadata(
            "partition",
            1000,
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli());
    final List<ServicePartitionMetadata> partitionList = Collections.singletonList(partition);

    ServiceMetadata serviceMetadata1 = new ServiceMetadata(name, partitionList);
    ServiceMetadata serviceMetadata2 = new ServiceMetadata(name + "2", partitionList);
    ServiceMetadata serviceMetadata3 = new ServiceMetadata(name, Collections.emptyList());

    assertThat(serviceMetadata1).isEqualTo(serviceMetadata1);
    assertThat(serviceMetadata1).isNotEqualTo(serviceMetadata2);
    assertThat(serviceMetadata1).isNotEqualTo(serviceMetadata3);

    Set<ServiceMetadata> set = new HashSet<>();
    set.add(serviceMetadata1);
    set.add(serviceMetadata2);
    set.add(serviceMetadata3);
    assertThat(set.size()).isEqualTo(3);
    assertThat(set).containsOnly(serviceMetadata1, serviceMetadata2, serviceMetadata3);
  }

  @Test
  public void testValidServiceMetadata() {
    final String name = "testService";
    final ServicePartitionMetadata partition =
        new ServicePartitionMetadata(
            "partition",
            1000,
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli());
    final List<ServicePartitionMetadata> partitionList = Collections.singletonList(partition);

    assertThatIllegalArgumentException().isThrownBy(() -> new ServiceMetadata("", partitionList));
    assertThatIllegalArgumentException().isThrownBy(() -> new ServiceMetadata(null, partitionList));
    assertThatIllegalArgumentException().isThrownBy(() -> new ServiceMetadata(name, null));
  }
}
