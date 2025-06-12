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
import com.slack.astra.metadata.cache.CacheSlotMetadata;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.hpa.HpaMetricMetadata;
import com.slack.astra.metadata.hpa.HpaMetricMetadataStore;
import com.slack.astra.metadata.replica.ReplicaMetadata;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
  private CacheSlotMetadataStore cacheSlotMetadataStore;

  private HpaMetricMetadataStore hpaMetricMetadataStore;
  private CacheNodeAssignmentStore cacheNodeAssignmentStore;
  private CacheNodeMetadataStore cacheNodeMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;

  @BeforeEach
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("ClusterHpaMetricServiceTest")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());

    replicaMetadataStore =
        spy(new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    cacheSlotMetadataStore =
        spy(new CacheSlotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    cacheNodeAssignmentStore =
        spy(new CacheNodeAssignmentStore(curatorFramework, metadataStoreConfig, meterRegistry));
    cacheNodeMetadataStore =
        spy(new CacheNodeMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    snapshotMetadataStore =
        spy(new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    hpaMetricMetadataStore =
        spy(new HpaMetricMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true));
  }

  @AfterEach
  public void shutdown() throws IOException {
    hpaMetricMetadataStore.close();
    cacheSlotMetadataStore.close();
    replicaMetadataStore.close();
    curatorFramework.unwrap().close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  void testLocking() throws Exception {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore,
            cacheSlotMetadataStore,
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
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  void oneReplicasetScaledown() {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore,
            cacheSlotMetadataStore,
            hpaMetricMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore);

    when(replicaMetadataStore.listSync())
        .thenReturn(
            List.of(
                new ReplicaMetadata("foo", "foo", "rep1", 1L, 0L, false),
                new ReplicaMetadata("bar", "bar", "rep2", 1L, 0L, false),
                new ReplicaMetadata("baz", "baz", "rep1", 1L, 0L, false)));

    when(cacheSlotMetadataStore.listSync())
        .thenReturn(
            List.of(
                new CacheSlotMetadata(
                    "foo",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    "localhost",
                    "rep1"),
                new CacheSlotMetadata(
                    "bar",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    "localhost",
                    "rep1"),
                new CacheSlotMetadata(
                    "baz",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    "localhost",
                    "rep2"),
                new CacheSlotMetadata(
                    "bal",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    "localhost",
                    "rep1")));

    clusterHpaMetricService.runOneIteration();

    AtomicReference<List<HpaMetricMetadata>> hpaMetricMetadataList = new AtomicReference<>();
    await()
        .until(
            () -> {
              hpaMetricMetadataList.set(hpaMetricMetadataStore.listSync());
              return hpaMetricMetadataList.get().size();
            },
            size -> size == 2);

    HpaMetricMetadata rep1Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep1")))
            .findFirst()
            .get();

    HpaMetricMetadata rep2Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep2")))
            .findFirst()
            .get();

    // 2 replicas, 3 slots
    assertThat(rep1Metadata.getValue()).isEqualTo(0.67);

    // 1 replica, 1 slot
    assertThat(rep2Metadata.getValue()).isEqualTo(1.0);
  }

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  void twoReplicasetScaledown() {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore,
            cacheSlotMetadataStore,
            hpaMetricMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore);

    when(replicaMetadataStore.listSync())
        .thenReturn(
            List.of(
                new ReplicaMetadata("foo", "foo", "rep1", 1L, 0L, false),
                new ReplicaMetadata("bar", "bar", "rep2", 1L, 0L, false)));

    when(cacheSlotMetadataStore.listSync())
        .thenReturn(
            List.of(
                new CacheSlotMetadata(
                    "foo",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    "localhost",
                    "rep1"),
                new CacheSlotMetadata(
                    "bar",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    "localhost",
                    "rep1"),
                new CacheSlotMetadata(
                    "baz",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    "localhost",
                    "rep2"),
                new CacheSlotMetadata(
                    "bal",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    "localhost",
                    "rep2")));

    clusterHpaMetricService.runOneIteration();

    AtomicReference<List<HpaMetricMetadata>> hpaMetricMetadataList = new AtomicReference<>();
    await()
        .until(
            () -> {
              hpaMetricMetadataList.set(hpaMetricMetadataStore.listSync());
              return hpaMetricMetadataList.get().size();
            },
            size -> size == 2);

    HpaMetricMetadata rep1Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep1")))
            .findFirst()
            .get();

    HpaMetricMetadata rep2Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep2")))
            .findFirst()
            .get();

    // only one should get a lock, the other should be 1 replica, 2 slots
    if (rep1Metadata.getValue().equals(1.0)) {
      assertThat(rep1Metadata.getValue()).isEqualTo(1.0);
      assertThat(rep2Metadata.getValue()).isEqualTo(0.5);
    } else {
      assertThat(rep1Metadata.getValue()).isEqualTo(0.5);
      assertThat(rep2Metadata.getValue()).isEqualTo(1.0);
    }
  }

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  void twoReplicasetScaleup() {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore,
            cacheSlotMetadataStore,
            hpaMetricMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore);

    when(replicaMetadataStore.listSync())
        .thenReturn(
            List.of(
                new ReplicaMetadata("foo", "foo", "rep1", 1L, 0L, false),
                new ReplicaMetadata("bar", "bar", "rep1", 1L, 0L, false),
                new ReplicaMetadata("baz", "bar", "rep2", 1L, 0L, false),
                new ReplicaMetadata("bal", "bar", "rep2", 1L, 0L, false)));

    when(cacheSlotMetadataStore.listSync())
        .thenReturn(
            List.of(
                new CacheSlotMetadata(
                    "foo",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    "localhost",
                    "rep1")));

    clusterHpaMetricService.runOneIteration();

    AtomicReference<List<HpaMetricMetadata>> hpaMetricMetadataList = new AtomicReference<>();
    await()
        .until(
            () -> {
              hpaMetricMetadataList.set(hpaMetricMetadataStore.listSync());
              return hpaMetricMetadataList.get().size();
            },
            size -> size == 2);

    HpaMetricMetadata rep1Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep1")))
            .findFirst()
            .get();

    HpaMetricMetadata rep2Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep2")))
            .findFirst()
            .get();

    // 2 replicas, 1 slot
    assertThat(rep1Metadata.getValue()).isEqualTo(2);

    // 2 replicas, 0 slots (will log an error and return a default no-op)
    assertThat(rep2Metadata.getValue()).isEqualTo(1);
  }

  @Test
  void testDemandFactorRounding() {
    assertThat(ClusterHpaMetricService.calculateDemandFactor(100, 98)).isEqualTo(0.98);
    assertThat(ClusterHpaMetricService.calculateDemandFactor(98, 100)).isEqualTo(1.03);
    assertThat(ClusterHpaMetricService.calculateDemandFactor(9999, 10000)).isEqualTo(1.01);
  }

  @Test
  public void testHpaScaleUp() throws Exception {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore,
            cacheSlotMetadataStore,
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
            cacheSlotMetadataStore,
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
            cacheSlotMetadataStore,
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
