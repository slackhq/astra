package com.slack.kaldb.logstore.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.kaldb.metadata.service.ServiceMetadata;
import com.slack.kaldb.metadata.service.ServiceMetadataStore;
import com.slack.kaldb.metadata.service.ServicePartitionMetadata;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KaldbDistributedQueryServicePartitionTest {

  private SimpleMeterRegistry metricsRegistry;
  private TestingServer testZKServer;
  private ZookeeperMetadataStoreImpl zkMetadataStore;
  private ServiceMetadataStore serviceMetadataStore;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();

    metricsRegistry = new SimpleMeterRegistry();
    testZKServer = new TestingServer();

    // Metadata store
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testZKServer.getConnectString())
            .setZkPathPrefix("indexerTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    zkMetadataStore = spy(ZookeeperMetadataStoreImpl.fromConfig(metricsRegistry, zkConfig));

    serviceMetadataStore = new ServiceMetadataStore(zkMetadataStore, true);
  }

  @After
  public void tearDown() throws Exception {
    serviceMetadataStore.close();
    zkMetadataStore.close();
    metricsRegistry.close();
    testZKServer.close();
  }

  @Test
  public void testOneServiceOnePartition() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final ServicePartitionMetadata partition =
        new ServicePartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("1"));
    final List<ServicePartitionMetadata> partitionConfigs = Collections.singletonList(partition);
    ServiceMetadata serviceMetadata =
        new ServiceMetadata(name, owner, throughputBytes, partitionConfigs);

    serviceMetadataStore.createSync(serviceMetadata);
    await().until(() -> serviceMetadataStore.listSync().size() == 1);

    assertThat(
            KaldbDistributedQueryService.getPartitionsForIndexName(serviceMetadataStore, name)
                .size())
        .isEqualTo(1);
    assertThat(
            KaldbDistributedQueryService.getPartitionsForIndexName(serviceMetadataStore, name)
                .iterator()
                .next())
        .isEqualTo("1");
  }

  @Test
  public void testOneServiceMultiplePartition() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final ServicePartitionMetadata partition =
        new ServicePartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("1", "2"));
    final List<ServicePartitionMetadata> partitionConfigs = Collections.singletonList(partition);
    ServiceMetadata serviceMetadata =
        new ServiceMetadata(name, owner, throughputBytes, partitionConfigs);

    serviceMetadataStore.createSync(serviceMetadata);
    await().until(() -> serviceMetadataStore.listSync().size() == 1);

    assertThat(
            KaldbDistributedQueryService.getPartitionsForIndexName(serviceMetadataStore, name)
                .size())
        .isEqualTo(2);
  }

  @Test
  public void testMultipleServices() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final ServicePartitionMetadata partition =
        new ServicePartitionMetadata(
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(90).toEpochMilli(),
            List.of("1"));
    final List<ServicePartitionMetadata> partitionConfigs = Collections.singletonList(partition);
    ServiceMetadata serviceMetadata =
        new ServiceMetadata(name, owner, throughputBytes, partitionConfigs);
    serviceMetadataStore.createSync(serviceMetadata);
    await().until(() -> serviceMetadataStore.listSync().size() == 1);

    serviceMetadata = new ServiceMetadata("testService2", owner, throughputBytes, partitionConfigs);
    serviceMetadataStore.createSync(serviceMetadata);
    await().until(() -> serviceMetadataStore.listSync().size() == 2);

    assertThat(
            KaldbDistributedQueryService.getPartitionsForIndexName(serviceMetadataStore, name)
                .size())
        .isEqualTo(1);
    assertThat(
            KaldbDistributedQueryService.getPartitionsForIndexName(serviceMetadataStore, name)
                .iterator()
                .next())
        .isEqualTo("1");

    assertThat(
            KaldbDistributedQueryService.getPartitionsForIndexName(
                    serviceMetadataStore, "testService2")
                .size())
        .isEqualTo(1);
    assertThat(
            KaldbDistributedQueryService.getPartitionsForIndexName(
                    serviceMetadataStore, "testService2")
                .iterator()
                .next())
        .isEqualTo("1");
  }
}
