package com.slack.astra.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.hpa.HpaMetricMetadata;
import com.slack.astra.metadata.hpa.HpaMetricMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HpaMetricPublisherServiceTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;
  private HpaMetricMetadataStore hpaMetricMetadataStore;
  private static EtcdCluster etcdCluster;
  private Client etcdClient;

  @BeforeEach
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();
    etcdCluster = TestEtcdClusterFactory.start();

    // Create etcd client
    etcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .namespace(
                ByteSequence.from(
                    "HpaMetricPublisherServiceTest", java.nio.charset.StandardCharsets.UTF_8))
            .build();

    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("HpaMetricPublisherServiceTest")
            .setEnabled(true)
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
            .build();

    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("HpaMetricPublisherServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .setZkCacheInitTimeoutMs(1000)
            .build();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .putStoreModes("DatasetMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SnapshotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("ReplicaMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("HpaMetricMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SearchMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheSlotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeAssignmentStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes(
                "FieldRedactionMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("PreprocessorMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryTaskMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .setZookeeperConfig(zkConfig)
            .setEtcdConfig(etcdConfig)
            .build();

    curatorFramework = CuratorBuilder.build(new SimpleMeterRegistry(), zkConfig);
    hpaMetricMetadataStore =
        spy(
            new HpaMetricMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true));
  }

  @AfterEach
  public void shutdown() throws IOException {
    hpaMetricMetadataStore.close();
    curatorFramework.unwrap().close();
    if (etcdClient != null) {
      etcdClient.close();
    }

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  void shouldRegisterMetersAsAdded() throws Exception {
    HpaMetricPublisherService hpaMetricPublisherService =
        new HpaMetricPublisherService(
            hpaMetricMetadataStore, meterRegistry, Metadata.HpaMetricMetadata.NodeRole.CACHE);
    hpaMetricPublisherService.startUp();

    // ZookeeperMetadataStore inits 9 of its own metrics
    assertThat(
            meterRegistry.getMeters().stream()
                .filter(meter -> meter.getId().getName().contains("astra_zk"))
                .toList()
                .size())
        .isEqualTo(9);

    hpaMetricMetadataStore.createSync(
        new HpaMetricMetadata("foo", Metadata.HpaMetricMetadata.NodeRole.CACHE, 1.0));

    await().until(() -> hpaMetricMetadataStore.listSync().size() == 1);
    await()
        .until(
            () ->
                meterRegistry.getMeters().stream()
                        .filter(
                            meter ->
                                meter.getId().getName().contains("astra_zk")
                                    || List.of("foo", "bar", "baz")
                                        .contains(meter.getId().getName()))
                        .toList()
                        .size()
                    == 10);

    hpaMetricMetadataStore.createSync(
        new HpaMetricMetadata("bar", Metadata.HpaMetricMetadata.NodeRole.INDEX, 1.0));

    await().until(() -> hpaMetricMetadataStore.listSync().size() == 2);
    await()
        .until(
            () ->
                meterRegistry.getMeters().stream()
                        .filter(
                            meter ->
                                meter.getId().getName().contains("astra_zk")
                                    || List.of("foo", "bar", "baz")
                                        .contains(meter.getId().getName()))
                        .toList()
                        .size()
                    == 10);

    hpaMetricMetadataStore.createSync(
        new HpaMetricMetadata("baz", Metadata.HpaMetricMetadata.NodeRole.CACHE, 0.0));

    await().until(() -> hpaMetricMetadataStore.listSync().size() == 3);
    await()
        .until(
            () ->
                meterRegistry.getMeters().stream()
                        .filter(
                            meter ->
                                meter.getId().getName().contains("astra_zk")
                                    || List.of("foo", "bar", "baz")
                                        .contains(meter.getId().getName()))
                        .toList()
                        .size()
                    == 11);
  }
}
