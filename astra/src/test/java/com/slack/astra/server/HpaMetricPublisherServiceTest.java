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

  @BeforeEach
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

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
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(zkConfig)
            .build();

    curatorFramework = CuratorBuilder.build(new SimpleMeterRegistry(), zkConfig);
    hpaMetricMetadataStore =
        spy(new HpaMetricMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true));
  }

  @AfterEach
  public void shutdown() throws IOException {
    hpaMetricMetadataStore.close();
    curatorFramework.unwrap().close();

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
