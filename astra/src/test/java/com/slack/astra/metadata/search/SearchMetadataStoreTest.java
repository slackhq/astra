package com.slack.astra.metadata.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.slack.astra.metadata.core.AstraPartitionedMetadata;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SearchMetadataStoreTest {
  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;
  private AstraConfigs.MetadataStoreConfig metadataStoreConfig;
  private SearchMetadataStore store;
  private static EtcdCluster etcdCluster;
  private Client etcdClient;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();
    etcdCluster = TestEtcdClusterFactory.start();

    // Create etcd client
    etcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .namespace(ByteSequence.from("Test", java.nio.charset.StandardCharsets.UTF_8))
            .build();

    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("Test")
            .setEnabled(true)
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
            .build();

    metadataStoreConfig =
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
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("Test")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(500)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .setEtcdConfig(etcdConfig)
            .build();
    this.curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (store != null) store.close();
    curatorFramework.unwrap().close();
    if (etcdClient != null) etcdClient.close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testSearchMetadataStoreUpdateSearchability() throws Exception {
    store =
        new SearchMetadataStore(
            curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true);
    SearchMetadata searchMetadata = new SearchMetadata("test", "snapshot", "http", false);
    assertThat(searchMetadata.isSearchable()).isFalse();
    store.createSync(searchMetadata);
    await().until(() -> store.listSync().contains(searchMetadata));

    store.updateSearchability(searchMetadata, true);

    // Confirm that this eventually becomes searchable
    String partition = searchMetadata.getPartition();
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> store.getSync(partition, "test").isSearchable());
  }

  @Test
  public void testSearchMetadataStoreIsNotUpdatable() throws Exception {
    store =
        new SearchMetadataStore(
            curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true);
    SearchMetadata searchMetadata = new SearchMetadata("test", "snapshot", "http");
    Throwable exAsync = catchThrowable(() -> store.updateAsync(searchMetadata));
    assertThat(exAsync).isInstanceOf(UnsupportedOperationException.class);

    Throwable exSync = catchThrowable(() -> store.updateSync(searchMetadata));
    assertThat(exSync).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testSearchMetadataPartitioning() {
    SearchMetadata searchMetadata = new SearchMetadata("test", "snapshot1", "test-url1");
    assertThat(searchMetadata.getPartition()).isEqualTo("test-url1");

    SearchMetadata anotherMetadata = new SearchMetadata("test2", "snapshot2", "test-url2");
    assertThat(anotherMetadata.getPartition()).isEqualTo("test-url2");

    assertThat(searchMetadata).isInstanceOf(AstraPartitionedMetadata.class);
  }
}
