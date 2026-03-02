package com.slack.astra.clusterManager;

import static com.slack.astra.clusterManager.ClusterHpaMetricService.CACHE_HPA_METRIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.hpa.HpaMetricMetadataStore;
import com.slack.astra.metadata.replica.ReplicaMetadata;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClusterHpaMetricServiceTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;

  private ReplicaMetadataStore replicaMetadataStore;

  private HpaMetricMetadataStore hpaMetricMetadataStore;
  private CacheNodeAssignmentStore cacheNodeAssignmentStore;
  private CacheNodeMetadataStore cacheNodeMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
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
                    "ClusterHpaMetricServiceTest", java.nio.charset.StandardCharsets.UTF_8))
            .build();

    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("ClusterHpaMetricServiceTest")
            .setEnabled(true)
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
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
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("ClusterHpaMetricServiceTest")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .setEtcdConfig(etcdConfig)
            .build();

    curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());

    replicaMetadataStore =
        spy(
            new ReplicaMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry));
    cacheNodeAssignmentStore =
        spy(
            new CacheNodeAssignmentStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry));
    cacheNodeMetadataStore =
        spy(
            new CacheNodeMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry));
    snapshotMetadataStore =
        spy(
            new SnapshotMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry));
    hpaMetricMetadataStore =
        spy(
            new HpaMetricMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true));
  }

  @AfterEach
  public void shutdown() throws IOException {
    hpaMetricMetadataStore.close();
    replicaMetadataStore.close();
    cacheNodeAssignmentStore.close();
    cacheNodeMetadataStore.close();
    snapshotMetadataStore.close();
    curatorFramework.unwrap().close();
    etcdClient.close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  void testLocking() throws Exception {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore,
            hpaMetricMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore);
    clusterHpaMetricService.CACHE_SCALEDOWN_LOCK = Duration.ofSeconds(2);

    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("foo")).isTrue();
    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("bar")).isFalse();
    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("foo")).isTrue();
    Thread.sleep(2000);
    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("bar")).isTrue();
    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("bar")).isTrue();
    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("foo")).isFalse();
  }

  @Test
  void testDemandFactorRounding() {
    // bytes requiring assignment, node capacity bytes
    assertThat(ClusterHpaMetricService.calculateDemandFactor(98, 100)).isEqualTo(0.98);
    assertThat(ClusterHpaMetricService.calculateDemandFactor(100, 98)).isEqualTo(1.03);
    assertThat(ClusterHpaMetricService.calculateDemandFactor(10000, 9999)).isEqualTo(1.01);
  }

  @Test
  public void testHpaScaleUp() throws Exception {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore,
            hpaMetricMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore);

    // Register 1 snapshot
    when(snapshotMetadataStore.listSync())
        .thenReturn(
            List.of(
                new SnapshotMetadata("snapshot2", 1L, 2L, 5L, "abcd", 10),
                new SnapshotMetadata("snapshot1", 1L, 2L, 5L, "abcd", 5)));

    // Register 1 replica associated with the snapshot
    when(replicaMetadataStore.listSync())
        .thenReturn(
            List.of(
                new ReplicaMetadata("replica2", "snapshot2", "rep1", 1L, 2L, false),
                new ReplicaMetadata("replica1", "snapshot1", "rep1", 1L, 2L, false)));

    // Register 2 cache nodes with lots of capacity
    when(cacheNodeMetadataStore.listSync())
        .thenReturn(List.of(new CacheNodeMetadata("node1", "node1.com", 10, "rep1")));

    // Assign replicas to cache nodes
    when(cacheNodeAssignmentStore.listSync())
        .thenReturn(
            List.of(
                new CacheNodeAssignment(
                    "jfkl",
                    "node1",
                    "snapshot2",
                    "replica2",
                    "rep1",
                    10,
                    Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE),
                new CacheNodeAssignment(
                    "abcd",
                    "node1",
                    "snapshot1",
                    "replica1",
                    "rep1",
                    5,
                    Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE)));

    clusterHpaMetricService.runOneIteration();

    // Assert scale down in HPA Store
    await()
        .timeout(10, TimeUnit.SECONDS)
        .until(() -> hpaMetricMetadataStore.hasSync(String.format(CACHE_HPA_METRIC_NAME, "rep1")));
    assertThat(hpaMetricMetadataStore.getSync(String.format(CACHE_HPA_METRIC_NAME, "rep1")).value)
        .isEqualTo(1.5);
  }

  @Test
  public void testHpaScaleDown() throws Exception {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore,
            hpaMetricMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore);

    // Register 1 snapshot
    when(snapshotMetadataStore.listSync())
        .thenReturn(List.of(new SnapshotMetadata("snapshot1", 1L, 2L, 5L, "abcd", 5)));

    // Register 1 replica associated with the snapshot
    when(replicaMetadataStore.listSync())
        .thenReturn(List.of(new ReplicaMetadata("replica1", "snapshot1", "rep1", 1L, 2L, false)));

    // Register 2 cache nodes with lots of capacity
    when(cacheNodeMetadataStore.listSync())
        .thenReturn(
            List.of(
                new CacheNodeMetadata("node2", "node2.com", 10, "rep1"),
                new CacheNodeMetadata("node1", "node1.com", 10, "rep1")));

    // Assign replicas to cache nodes
    when(cacheNodeAssignmentStore.listSync())
        .thenReturn(
            List.of(
                new CacheNodeAssignment(
                    "abcd",
                    "node1",
                    "snapshot1",
                    "replica1",
                    "rep1",
                    5,
                    Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE)));

    clusterHpaMetricService.runOneIteration();

    // Assert scale down in HPA Store
    await()
        .timeout(10, TimeUnit.SECONDS)
        .until(() -> hpaMetricMetadataStore.hasSync(String.format(CACHE_HPA_METRIC_NAME, "rep1")));
    assertThat(hpaMetricMetadataStore.getSync(String.format(CACHE_HPA_METRIC_NAME, "rep1")).value)
        .isEqualTo(0.25);
  }

  @Test
  public void testHpaReplicaSetFiltering() throws Exception {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore,
            hpaMetricMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore);

    // Register snapshot of size 15
    when(snapshotMetadataStore.listSync())
        .thenReturn(List.of(new SnapshotMetadata("snapshot1", 1L, 2L, 5L, "abcd", 15)));

    // Register 2 replicas for rep1 and rep2
    when(replicaMetadataStore.listSync())
        .thenReturn(
            List.of(
                new ReplicaMetadata("replica2", "snapshot1", "rep1", 1L, 2L, false),
                new ReplicaMetadata("replica1", "snapshot1", "rep2", 1L, 2L, false)));

    // Register 2 cache nodes (rep1, rep2), of size 10 each
    when(cacheNodeMetadataStore.listSync())
        .thenReturn(
            List.of(
                new CacheNodeMetadata("node1", "node1.com", 10, "rep1"),
                new CacheNodeMetadata("node2", "node1.com", 10, "rep2")));

    clusterHpaMetricService.runOneIteration();

    // assert scale up for both replicaSets
    await()
        .timeout(10, TimeUnit.SECONDS)
        .until(() -> hpaMetricMetadataStore.hasSync(String.format(CACHE_HPA_METRIC_NAME, "rep1")));
    assertThat(hpaMetricMetadataStore.getSync(String.format(CACHE_HPA_METRIC_NAME, "rep1")).value)
        .isEqualTo(1.5);

    await()
        .timeout(10, TimeUnit.SECONDS)
        .until(() -> hpaMetricMetadataStore.hasSync(String.format(CACHE_HPA_METRIC_NAME, "rep2")));
    assertThat(hpaMetricMetadataStore.getSync(String.format(CACHE_HPA_METRIC_NAME, "rep2")).value)
        .isEqualTo(1.5);
  }
}
