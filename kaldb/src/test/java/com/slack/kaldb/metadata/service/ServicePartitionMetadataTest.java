package com.slack.kaldb.metadata.service;

import static com.slack.kaldb.metadata.service.ServicePartitionMetadata.findPartitionsToQuery;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ServicePartitionMetadataTest {

  private SimpleMeterRegistry metricsRegistry;
  private MetadataStore zkMetadataStore;
  private ServiceMetadataStore serviceMetadataStore;
  private TestingServer testZKServer;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();

    metricsRegistry = new SimpleMeterRegistry();
    testZKServer = new TestingServer();

    // Metadata store
    com.slack.kaldb.proto.config.KaldbConfigs.ZookeeperConfig zkConfig =
        com.slack.kaldb.proto.config.KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testZKServer.getConnectString())
            .setZkPathPrefix("servicePartitionMetadataTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    zkMetadataStore = ZookeeperMetadataStoreImpl.fromConfig(metricsRegistry, zkConfig);
    serviceMetadataStore = new ServiceMetadataStore(zkMetadataStore, true);
  }

  @After
  public void tearDown() throws Exception {
    serviceMetadataStore.close();
    zkMetadataStore.close();
    testZKServer.close();
    metricsRegistry.close();
  }

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

  @Test
  public void testOneServiceOnePartition() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final ServicePartitionMetadata partition = new ServicePartitionMetadata(100, 200, List.of("1"));

    ServiceMetadata serviceMetadata =
        new ServiceMetadata(name, owner, throughputBytes, List.of(partition));

    serviceMetadataStore.createSync(serviceMetadata);
    await().until(() -> serviceMetadataStore.getCached().size() == 1);

    // Start and end time within query window
    List<ServicePartitionMetadata> partitionMetadata =
        findPartitionsToQuery(serviceMetadataStore, 101, 199, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    // End time partially overlapping query window
    partitionMetadata = findPartitionsToQuery(serviceMetadataStore, 1, 150, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    // End time overlapping entire query window
    partitionMetadata = findPartitionsToQuery(serviceMetadataStore, 1, 250, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    // Start time at edge of query window
    partitionMetadata = findPartitionsToQuery(serviceMetadataStore, 200, 250, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    // Start and end time outside of query window
    partitionMetadata = findPartitionsToQuery(serviceMetadataStore, 201, 250, name);
    assertThat(partitionMetadata.size()).isEqualTo(0);
  }

  @Test
  public void testOneServiceMultipleWindows() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final ServicePartitionMetadata partition1 =
        new ServicePartitionMetadata(100, 200, List.of("1"));

    final ServicePartitionMetadata partition2 =
        new ServicePartitionMetadata(201, 300, List.of("2", "3"));

    ServiceMetadata serviceMetadata =
        new ServiceMetadata(name, owner, throughputBytes, List.of(partition1, partition2));

    serviceMetadataStore.createSync(serviceMetadata);
    await().until(() -> serviceMetadataStore.getCached().size() == 1);

    // Fetch first partition between time 101 and 199
    List<ServicePartitionMetadata> partitionMetadata =
        findPartitionsToQuery(serviceMetadataStore, 101, 199, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).startTimeEpochMs).isEqualTo(100);
    assertThat(partitionMetadata.get(0).endTimeEpochMs).isEqualTo(200);

    // Fetch second partition between time 201 and 300
    partitionMetadata = findPartitionsToQuery(serviceMetadataStore, 201, 300, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(2);
    assertThat(partitionMetadata.get(0).startTimeEpochMs).isEqualTo(201);
    assertThat(partitionMetadata.get(0).endTimeEpochMs).isEqualTo(300);

    // Fetch both partitions
    partitionMetadata = findPartitionsToQuery(serviceMetadataStore, 100, 202, name);
    assertThat(partitionMetadata.size()).isEqualTo(2);

    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).startTimeEpochMs).isEqualTo(100);
    assertThat(partitionMetadata.get(0).endTimeEpochMs).isEqualTo(200);

    assertThat(partitionMetadata.get(1).partitions.size()).isEqualTo(2);
    assertThat(partitionMetadata.get(1).startTimeEpochMs).isEqualTo(201);
    assertThat(partitionMetadata.get(1).endTimeEpochMs).isEqualTo(300);
  }

  @Test
  public void testMultipleServicesOneTimeRange() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final ServicePartitionMetadata partition = new ServicePartitionMetadata(100, 200, List.of("1"));

    ServiceMetadata serviceMetadata =
        new ServiceMetadata(name, owner, throughputBytes, List.of(partition));

    serviceMetadataStore.createSync(serviceMetadata);
    await().until(() -> serviceMetadataStore.getCached().size() == 1);

    final String name1 = "testService1";
    final String owner1 = "serviceOwner1";
    final long throughputBytes1 = 1;
    final ServicePartitionMetadata partition1 =
        new ServicePartitionMetadata(100, 200, List.of("2"));

    ServiceMetadata serviceMetadata1 =
        new ServiceMetadata(name1, owner1, throughputBytes1, List.of(partition1));

    serviceMetadataStore.createSync(serviceMetadata1);
    await().until(() -> serviceMetadataStore.getCached().size() == 2);

    List<ServicePartitionMetadata> partitionMetadata =
        findPartitionsToQuery(serviceMetadataStore, 101, 199, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.get(0)).isEqualTo("1");
  }
}
