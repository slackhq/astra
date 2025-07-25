package com.slack.astra.clusterManager;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
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
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CacheNodeSearchabilityServiceTest {
  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private CacheNodeAssignmentStore cacheNodeAssignmentStore;
  private AstraConfigs.ManagerConfig managerConfig;
  private CacheNodeMetadataStore cacheNodeMetadataStore;
  private static EtcdCluster etcdCluster;
  private Client etcdClient;

  @BeforeEach
  public void setUp() throws Exception {
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
                    "CacheNodeAssignmentServiceTest", java.nio.charset.StandardCharsets.UTF_8))
            .build();

    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("CacheNodeAssignmentServiceTest")
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
                    .setZkPathPrefix("CacheNodeAssignmentServiceTest")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .setEtcdConfig(etcdConfig)
            .build();

    AstraConfigs.ManagerConfig.CacheNodeSearchabilityServiceConfig
        cacheNodeSearchabilityServiceConfig =
            AstraConfigs.ManagerConfig.CacheNodeSearchabilityServiceConfig.newBuilder()
                .setSchedulePeriodMins(1)
                .build();

    managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setCacheNodeSearchabilityServiceConfig(cacheNodeSearchabilityServiceConfig)
            .build();

    curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
    searchMetadataStore =
        spy(
            new SearchMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true));
    cacheNodeMetadataStore =
        spy(
            new CacheNodeMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry));
    cacheNodeAssignmentStore =
        spy(
            new CacheNodeAssignmentStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry));
    snapshotMetadataStore =
        spy(
            new SnapshotMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry));
  }

  @AfterEach
  public void tearDown() throws IOException {
    meterRegistry.close();
    testingServer.close();
    searchMetadataStore.close();
    cacheNodeAssignmentStore.close();
    cacheNodeMetadataStore.close();
    snapshotMetadataStore.close();
    curatorFramework.unwrap().close();
    etcdClient.close();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithNoCacheNodes() throws Exception {
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithNoUnsearchableCacheNodes() throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", true));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    assertThat(cacheNodeMetadata.searchable).isTrue();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithNoAssignments() throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", false));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    assertThat(cacheNodeMetadata.searchable).isFalse();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithNoSearchMetadata() throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", false));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            "snapshot-id",
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    assertThat(cacheNodeMetadata.searchable).isTrue();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithSearchMetadata() throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", false));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            "snapshot-id",
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE));
    searchMetadataStore.createSync(
        new SearchMetadata("test-name", "snapshot-id", "testhostname", false));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    SearchMetadata searchMetadata = searchMetadataStore.getSync("testhostname", "test-name");
    assertThat(cacheNodeMetadata.searchable).isTrue();
    assertThat(searchMetadata.isSearchable()).isTrue();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithSearchMetadataInLoadingState() throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", false));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            "snapshot-id",
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING));
    searchMetadataStore.createSync(
        new SearchMetadata("test-name", "snapshot-id", "test-url", false));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    SearchMetadata searchMetadata = searchMetadataStore.getSync("test-url", "test-name");
    assertThat(cacheNodeMetadata.searchable).isFalse();
    assertThat(searchMetadata.isSearchable()).isFalse();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithSearchMetadataInEvictingState()
      throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", false));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            "snapshot-id",
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICTING));
    searchMetadataStore.createSync(
        new SearchMetadata("test-name", "snapshot-id", "http://test-url", false));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    SearchMetadata searchMetadata = searchMetadataStore.getSync("test-url", "test-name");
    assertThat(cacheNodeMetadata.searchable).isFalse();
    assertThat(searchMetadata.isSearchable()).isFalse();
  }
}
