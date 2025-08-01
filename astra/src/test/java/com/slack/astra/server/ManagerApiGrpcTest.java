package com.slack.astra.server;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.server.ManagerApiGrpc.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.astra.clusterManager.ReplicaRestoreService;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.core.InternalMetadataStoreException;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.manager_api.ManagerApi;
import com.slack.astra.proto.manager_api.ManagerApiServiceGrpc;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.testlib.MetricsUtil;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import com.slack.astra.util.GrpcCleanupExtension;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ManagerApiGrpcTest {

  @RegisterExtension public final GrpcCleanupExtension grpcCleanup = new GrpcCleanupExtension();

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private AsyncCuratorFramework curatorFramework;
  private DatasetMetadataStore datasetMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;
  private ReplicaRestoreService replicaRestoreService;
  private FieldRedactionMetadataStore fieldRedactionMetadataStore;
  private ManagerApiServiceGrpc.ManagerApiServiceBlockingStub managerApiStub;
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
                ByteSequence.from("ManagerApiGrpcTest", java.nio.charset.StandardCharsets.UTF_8))
            .build();

    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("ManagerApiGrpcTest")
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
                    .setEnabled(true)
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("ManagerApiGrpcTest")
                    .setZkSessionTimeoutMs(30000)
                    .setZkConnectionTimeoutMs(30000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .setEtcdConfig(etcdConfig)
            .build();

    curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
    datasetMetadataStore =
        spy(
            new DatasetMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true));
    snapshotMetadataStore =
        spy(
            new SnapshotMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry));
    replicaMetadataStore =
        spy(
            new ReplicaMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry));
    fieldRedactionMetadataStore =
        spy(
            new FieldRedactionMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true));

    AstraConfigs.ManagerConfig.ReplicaRestoreServiceConfig replicaRecreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaRestoreServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1"))
            .setMaxReplicasPerRequest(200)
            .setReplicaLifespanMins(60)
            .setSchedulePeriodMins(30)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaRestoreServiceConfig(replicaRecreationServiceConfig)
            .build();

    replicaRestoreService =
        new ReplicaRestoreService(replicaMetadataStore, meterRegistry, managerConfig);

    grpcCleanup.register(
        InProcessServerBuilder.forName(this.getClass().toString())
            .directExecutor()
            .addService(
                new ManagerApiGrpc(
                    datasetMetadataStore,
                    snapshotMetadataStore,
                    replicaRestoreService,
                    fieldRedactionMetadataStore))
            .build()
            .start());
    ManagedChannel channel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName(this.getClass().toString()).directExecutor().build());

    managerApiStub = ManagerApiServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  public void tearDown() throws Exception {
    replicaRestoreService.stopAsync();
    replicaRestoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);

    replicaMetadataStore.close();
    snapshotMetadataStore.close();
    datasetMetadataStore.close();
    fieldRedactionMetadataStore.close();
    curatorFramework.unwrap().close();
    if (etcdClient != null) {
      etcdClient.close();
    }

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldCreateAndGetNewDataset() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner(datasetOwner)
            .build());

    Metadata.DatasetMetadata getDatasetMetadataResponse =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());
    assertThat(getDatasetMetadataResponse.getName()).isEqualTo(datasetName);
    assertThat(getDatasetMetadataResponse.getOwner()).isEqualTo(datasetOwner);
    assertThat(getDatasetMetadataResponse.getThroughputBytes()).isEqualTo(0);
    assertThat(getDatasetMetadataResponse.getPartitionConfigsList().size()).isEqualTo(0);

    DatasetMetadata datasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(datasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(datasetMetadata.getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorCreatingDuplicateDatasetName() {
    String datasetName = "testDataset";
    String datasetOwner1 = "testOwner1";
    String datasetOwner2 = "testOwner2";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner(datasetOwner1)
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner(datasetOwner2)
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    DatasetMetadata datasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(datasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.getOwner()).isEqualTo(datasetOwner1);
    assertThat(datasetMetadata.getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorCreatingWithInvalidDatasetNames() {
    String datasetOwner = "testOwner";

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName("")
                            .setOwner(datasetOwner)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable1.getStatus().getDescription()).isEqualTo("name can't be null or empty.");

    StatusRuntimeException throwable2 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName("/")
                            .setOwner(datasetOwner)
                            .build()));
    assertThat(throwable2.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    StatusRuntimeException throwable3 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(".")
                            .setOwner(datasetOwner)
                            .build()));
    assertThat(throwable3.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorWithEmptyOwnerInformation() {
    String datasetName = "testDataset";

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner("")
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable.getStatus().getDescription()).isEqualTo("owner must not be null or blank");

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldUpdateExistingDataset() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    String serviceNamePattern = "serviceNamePattern";
    String updatedServiceNamePattern = "updatedServiceNamePattern";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner(datasetOwner)
            .setServiceNamePattern(serviceNamePattern)
            .build());

    String updatedDatasetOwner = "testOwnerUpdated";
    Metadata.DatasetMetadata updatedDatasetResponse =
        managerApiStub.updateDatasetMetadata(
            ManagerApi.UpdateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(updatedDatasetOwner)
                .setServiceNamePattern(serviceNamePattern)
                .build());

    assertThat(updatedDatasetResponse.getName()).isEqualTo(datasetName);
    assertThat(updatedDatasetResponse.getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(updatedDatasetResponse.getServiceNamePattern()).isEqualTo(serviceNamePattern);
    assertThat(updatedDatasetResponse.getThroughputBytes()).isEqualTo(0);
    assertThat(updatedDatasetResponse.getPartitionConfigsList().size()).isEqualTo(0);

    AtomicReference<DatasetMetadata> datasetMetadata = new AtomicReference<>();
    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return datasetMetadata.get().getOwner().equals(updatedDatasetOwner);
            });

    assertThat(datasetMetadata.get().getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.get().getServiceNamePattern()).isEqualTo(serviceNamePattern);
    assertThat(datasetMetadata.get().getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(datasetMetadata.get().getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.get().getPartitionConfigs().size()).isEqualTo(0);

    Metadata.DatasetMetadata updatedServiceNamePatternResponse =
        managerApiStub.updateDatasetMetadata(
            ManagerApi.UpdateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(updatedDatasetOwner)
                .setServiceNamePattern(updatedServiceNamePattern)
                .build());

    assertThat(updatedServiceNamePatternResponse.getName()).isEqualTo(datasetName);
    assertThat(updatedServiceNamePatternResponse.getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(updatedServiceNamePatternResponse.getServiceNamePattern())
        .isEqualTo(updatedServiceNamePattern);
    assertThat(updatedServiceNamePatternResponse.getThroughputBytes()).isEqualTo(0);
    assertThat(updatedServiceNamePatternResponse.getPartitionConfigsList().size()).isEqualTo(0);

    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return Objects.equals(
                  datasetMetadata.get().getServiceNamePattern(), updatedServiceNamePattern);
            });

    datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
    assertThat(datasetMetadata.get().getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.get().getServiceNamePattern()).isEqualTo(updatedServiceNamePattern);
    assertThat(datasetMetadata.get().getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(datasetMetadata.get().getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.get().getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldMigrateExistingDataset() throws InterruptedException {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";
    String serviceNamePattern = "serviceNamePattern";

    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("ManagerApiGrpcTest")
            .setEnabled(true)
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
            .build();

    AstraConfigs.MetadataStoreConfig metadataStoreConfigWithEtcdCreates =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .putStoreModes("DatasetMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setEnabled(true)
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("ManagerApiGrpcTest")
                    .setZkSessionTimeoutMs(30000)
                    .setZkConnectionTimeoutMs(30000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .setEtcdConfig(etcdConfig)
            .build();

    AstraConfigs.MetadataStoreConfig metadataStoreConfigWithZKCreates =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .putStoreModes("DatasetMetadataStore", AstraConfigs.MetadataStoreMode.ZOOKEEPER_CREATES)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setEnabled(true)
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("ManagerApiGrpcTest")
                    .setZkSessionTimeoutMs(30000)
                    .setZkConnectionTimeoutMs(30000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .setEtcdConfig(etcdConfig)
            .build();

    AsyncCuratorFramework localCuratorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfigWithZKCreates.getZookeeperConfig());

    // Create a dataset which will be stored in ZK (stored in ETCD first)
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner(datasetOwner)
            .setServiceNamePattern(serviceNamePattern)
            .build());

    AtomicReference<DatasetMetadata> originalDataset = new AtomicReference<>();
    await()
        .until(
            () -> {
              originalDataset.set(datasetMetadataStore.getSync(datasetName));
              return originalDataset.get().getOwner().equals(datasetOwner);
            });

    assertThat(originalDataset.get().getName()).isEqualTo(datasetName);
    assertThat(originalDataset.get().getServiceNamePattern()).isEqualTo(serviceNamePattern);
    assertThat(originalDataset.get().getOwner()).isEqualTo(datasetOwner);
    assertThat(originalDataset.get().getThroughputBytes()).isEqualTo(0);
    assertThat(originalDataset.get().getPartitionConfigs().size()).isEqualTo(0);

    // Create separate Zookeeper-only and Etcd-only stores to test individual stores
    DatasetMetadataStore zkOnlyStore =
        new DatasetMetadataStore(
            localCuratorFramework, null, metadataStoreConfigWithZKCreates, meterRegistry, true);

    DatasetMetadataStore etcdOnlyStore =
        new DatasetMetadataStore(
            null, etcdClient, metadataStoreConfigWithEtcdCreates, meterRegistry, true);

    // For the migration test, we need to simulate having data in ZK first
    // Since our test setup uses ETCD_CREATES mode, we need to manually create in ZK
    zkOnlyStore.createSync(originalDataset.get());

    // Verify dataset exists in ZK before migration
    AtomicReference<DatasetMetadata> zkDataset = new AtomicReference<>();
    await()
        .until(
            () -> {
              zkDataset.set(datasetMetadataStore.getSync(datasetName));
              return zkDataset.get().getOwner().equals(datasetOwner);
            });

    assertThat(zkDataset.get().getName()).isEqualTo(datasetName);
    assertThat(zkDataset.get().getServiceNamePattern()).isEqualTo(serviceNamePattern);
    assertThat(zkDataset.get().getOwner()).isEqualTo(datasetOwner);
    assertThat(zkDataset.get().getThroughputBytes()).isEqualTo(0);
    assertThat(zkDataset.get().getPartitionConfigs().size()).isEqualTo(0);

    // Perform the migration
    ManagerApi.MigrateZKDatasetMetadataStoreToEtcdResponse migrateResponse =
        managerApiStub.migrateZKDatasetMetadataStoreToEtcd(
            ManagerApi.MigrateZKDatasetMetadataStoreToEtcdRequest.newBuilder()
                .setDryRun(false)
                .build());

    // Verify migration response
    assertThat(migrateResponse.getStatus()).isEqualTo("SUCCESS");
    // Since Etcd already had the dataset due to the test using ETCD_CREATES, verifying the size
    // ensures it got deleted and then added again, instead of getting added twice
    assertThat(migrateResponse.getDatasetMetadataList()).hasSize(1);
    assertThat(migrateResponse.getDatasetMetadata(0).getName()).isEqualTo(datasetName);

    // Verify dataset was deleted from Zookeeper
    await().until(() -> zkOnlyStore.listSync().isEmpty());
    assertThat(zkOnlyStore.hasSync(datasetName)).isFalse();

    // Verify dataset was added to Etcd
    assertThat(etcdOnlyStore.hasSync(datasetName)).isTrue();
    DatasetMetadata etcdDataset = etcdOnlyStore.getSync(datasetName);
    assertThat(etcdDataset.getName()).isEqualTo(datasetName);
    assertThat(etcdDataset.getOwner()).isEqualTo(datasetOwner);
    assertThat(etcdDataset.getServiceNamePattern()).isEqualTo(serviceNamePattern);

    // Clean up
    localCuratorFramework.unwrap().close();
    zkOnlyStore.close();
    etcdOnlyStore.close();
  }

  @Test
  public void shouldErrorGettingNonexistentDataset() {
    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.getDatasetMetadata(
                        ManagerApi.GetDatasetMetadataRequest.newBuilder().setName("foo").build()));
    Status status = throwable.getStatus();
    assertThat(status.getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldUpdatePartitionAssignments() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    Metadata.DatasetMetadata initialDatasetRequest =
        managerApiStub.createDatasetMetadata(
            ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(datasetOwner)
                .build());
    assertThat(initialDatasetRequest.getPartitionConfigsList().size()).isEqualTo(0);

    long nowMs = Instant.now().toEpochMilli();
    long throughputBytes = 10;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(throughputBytes)
            .addAllPartitionIds(List.of("1", "2"))
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore.listSync().size() == 1
                    && datasetMetadataStore.listSync().get(0).getThroughputBytes()
                        == throughputBytes);

    Metadata.DatasetMetadata firstAssignment =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());

    assertThat(firstAssignment.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstAssignment.getPartitionConfigsList().size()).isEqualTo(1);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    DatasetMetadata firstDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(firstDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(firstDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(firstDatasetMetadata.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstDatasetMetadata.getPartitionConfigs().size()).isEqualTo(1);

    // only update the partition assignment, leaving throughput
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(-1)
            .addAllPartitionIds(List.of("3", "4", "5"))
            .build());

    AtomicReference<Metadata.DatasetMetadata> secondAssignment = new AtomicReference<>();
    await()
        .until(
            () -> {
              secondAssignment.set(
                  managerApiStub.getDatasetMetadata(
                      ManagerApi.GetDatasetMetadataRequest.newBuilder()
                          .setName(datasetName)
                          .build()));
              return secondAssignment.get().getThroughputBytes() == throughputBytes;
            });

    assertThat(secondAssignment.get().getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(secondAssignment.get().getPartitionConfigsList().size()).isEqualTo(2);
    assertThat(secondAssignment.get().getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(secondAssignment.get().getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(secondAssignment.get().getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(secondAssignment.get().getPartitionConfigsList().get(1).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(secondAssignment.get().getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    DatasetMetadata secondDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(secondDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(secondDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(secondDatasetMetadata.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(secondDatasetMetadata.getPartitionConfigs().size()).isEqualTo(2);

    // only update the throughput, leaving the partition assignments
    long updatedThroughputBytes = 12;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(updatedThroughputBytes)
            .build());

    AtomicReference<Metadata.DatasetMetadata> thirdAssignment = new AtomicReference<>();
    await()
        .until(
            () -> {
              thirdAssignment.set(
                  managerApiStub.getDatasetMetadata(
                      ManagerApi.GetDatasetMetadataRequest.newBuilder()
                          .setName(datasetName)
                          .build()));
              return thirdAssignment.get().getThroughputBytes() == updatedThroughputBytes;
            });

    assertThat(thirdAssignment.get().getThroughputBytes()).isEqualTo(updatedThroughputBytes);
    assertThat(thirdAssignment.get().getPartitionConfigsList().size()).isEqualTo(2);
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(thirdAssignment.get().getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(1).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    DatasetMetadata thirdDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(thirdDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(thirdDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(thirdDatasetMetadata.getThroughputBytes()).isEqualTo(updatedThroughputBytes);
    assertThat(thirdDatasetMetadata.getPartitionConfigs().size()).isEqualTo(2);
  }

  @Test
  public void shouldErrorUpdatingPartitionAssignmentNonexistentDataset() {
    String datasetName = "testDataset";
    List<String> partitionList = List.of("1", "2");

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(-1)
                            .addAllPartitionIds(partitionList)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldListExistingDatasets() {
    String datasetName1 = "testDataset1";
    String datasetOwner1 = "testOwner1";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName1)
            .setOwner(datasetOwner1)
            .build());

    String datasetName2 = "testDataset2";
    String datasetOwner2 = "testOwner2";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName2)
            .setOwner(datasetOwner2)
            .build());

    ManagerApi.ListDatasetMetadataResponse listDatasetMetadataResponse =
        managerApiStub.listDatasetMetadata(
            ManagerApi.ListDatasetMetadataRequest.newBuilder().build());

    assertThat(
        listDatasetMetadataResponse
            .getDatasetMetadataList()
            .containsAll(
                List.of(
                    Metadata.DatasetMetadata.newBuilder()
                        .setName(datasetName1)
                        .setOwner(datasetOwner1)
                        .setThroughputBytes(0)
                        .build(),
                    Metadata.DatasetMetadata.newBuilder()
                        .setName(datasetName2)
                        .setOwner(datasetOwner2)
                        .setThroughputBytes(0)
                        .build())));

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(2);
    assertThat(
        AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore)
            .containsAll(
                List.of(
                    new DatasetMetadata(
                        datasetName1, datasetOwner1, 0, Collections.emptyList(), datasetName1),
                    new DatasetMetadata(
                        datasetName2, datasetOwner2, 0, Collections.emptyList(), datasetName2))));
  }

  @Test
  public void shouldHandleZkErrorsGracefully() {
    String datasetName = "testZkErrorsDataset";
    String datasetOwner = "testZkErrorsOwner";
    String errorString = "zkError";

    doThrow(new InternalMetadataStoreException(errorString))
        .when(datasetMetadataStore)
        .createSync(
            eq(
                new DatasetMetadata(
                    datasetName, datasetOwner, 0L, Collections.emptyList(), datasetName)));

    StatusRuntimeException throwableCreate =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner(datasetOwner)
                            .setServiceNamePattern(datasetName)
                            .build()));

    assertThat(throwableCreate.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwableCreate.getStatus().getDescription()).isEqualTo(errorString);

    doThrow(new InternalMetadataStoreException(errorString))
        .when(datasetMetadataStore)
        .updateSync(
            eq(
                new DatasetMetadata(
                    datasetName, datasetOwner, 0L, Collections.emptyList(), datasetName)));

    StatusRuntimeException throwableUpdate =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updateDatasetMetadata(
                        ManagerApi.UpdateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner(datasetOwner)
                            .build()));

    assertThat(throwableUpdate.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwableUpdate.getStatus().getDescription()).contains(datasetName);

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldFetchSnapshotsWithinTimeframeAndPartition() {
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    SnapshotMetadata overlapsStartTimeIncluded =
        new SnapshotMetadata("a", startTime, startTime + 6, 0, "a", 0);
    SnapshotMetadata overlapsStartTimeExcluded =
        new SnapshotMetadata("b", startTime, startTime + 6, 0, "b", 0);

    SnapshotMetadata fullyOverlapsStartEndTimeIncluded =
        new SnapshotMetadata("c", startTime + 4, startTime + 11, 0, "a", 0);
    SnapshotMetadata fullyOverlapsStartEndTimeExcluded =
        new SnapshotMetadata("d", startTime + 4, startTime + 11, 0, "b", 0);

    SnapshotMetadata partiallyOverlapsStartEndTimeIncluded =
        new SnapshotMetadata("e", startTime + 4, startTime + 5, 0, "a", 0);
    SnapshotMetadata partiallyOverlapsStartEndTimeExcluded =
        new SnapshotMetadata("f", startTime + 4, startTime + 5, 0, "b", 0);

    SnapshotMetadata overlapsEndTimeIncluded =
        new SnapshotMetadata("g", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata overlapsEndTimeExcluded =
        new SnapshotMetadata("h", startTime + 10, startTime + 15, 0, "b", 0);

    SnapshotMetadata notWithinStartEndTimeExcluded1 =
        new SnapshotMetadata("i", startTime, startTime + 4, 0, "a", 0);
    SnapshotMetadata notWithinStartEndTimeExcluded2 =
        new SnapshotMetadata("j", startTime + 11, startTime + 15, 0, "a", 0);

    DatasetMetadata datasetWithDataInPartitionA =
        new DatasetMetadata(
            "foo",
            "a",
            1,
            List.of(new DatasetPartitionMetadata(startTime + 5, startTime + 6, List.of("a"))),
            "fooService");

    datasetMetadataStore.createSync(datasetWithDataInPartitionA);

    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    List<SnapshotMetadata> snapshotsWithData =
        ManagerApiGrpc.calculateRequiredSnapshots(
            Arrays.asList(
                overlapsEndTimeIncluded,
                overlapsEndTimeExcluded,
                partiallyOverlapsStartEndTimeIncluded,
                partiallyOverlapsStartEndTimeExcluded,
                fullyOverlapsStartEndTimeIncluded,
                fullyOverlapsStartEndTimeExcluded,
                overlapsStartTimeIncluded,
                overlapsStartTimeExcluded,
                notWithinStartEndTimeExcluded1,
                notWithinStartEndTimeExcluded2),
            datasetMetadataStore,
            start,
            end,
            "foo");

    assertThat(snapshotsWithData.size()).isEqualTo(4);
    assertThat(
            snapshotsWithData.containsAll(
                Arrays.asList(
                    overlapsStartTimeIncluded,
                    fullyOverlapsStartEndTimeIncluded,
                    partiallyOverlapsStartEndTimeIncluded,
                    overlapsEndTimeIncluded)))
        .isTrue();
  }

  @Test
  public void shouldRestoreReplicaSinglePartition() {
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    SnapshotMetadata snapshotIncluded =
        new SnapshotMetadata("g", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata snapshotExcluded =
        new SnapshotMetadata("h", startTime + 10, startTime + 15, 0, "b", 0);

    snapshotMetadataStore.createSync(snapshotIncluded);
    snapshotMetadataStore.createSync(snapshotExcluded);

    DatasetMetadata serviceWithDataInPartitionA =
        new DatasetMetadata(
            "foo",
            "a",
            1,
            List.of(new DatasetPartitionMetadata(startTime + 5, startTime + 6, List.of("a"))),
            "fooService");

    datasetMetadataStore.createSync(serviceWithDataInPartitionA);

    await().until(() -> datasetMetadataStore.listSync().size() == 1);
    await().until(() -> snapshotMetadataStore.listSync().size() == 2);

    managerApiStub.restoreReplica(
        ManagerApi.RestoreReplicaRequest.newBuilder()
            .setServiceName("foo")
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    await()
        .until(
            () -> MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry) == 1);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(0);
  }

  @Test
  public void shouldRestoreReplicasMultiplePartitions() {
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    SnapshotMetadata snapshotIncluded =
        new SnapshotMetadata("a", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata snapshotIncluded2 =
        new SnapshotMetadata("b", startTime + 10, startTime + 15, 0, "b", 0);
    SnapshotMetadata snapshotExcluded =
        new SnapshotMetadata("c", startTime + 10, startTime + 15, 0, "c", 0);

    snapshotMetadataStore.createSync(snapshotIncluded);
    snapshotMetadataStore.createSync(snapshotIncluded2);
    snapshotMetadataStore.createSync(snapshotExcluded);

    DatasetMetadata serviceWithDataInPartitionA =
        new DatasetMetadata(
            "foo",
            "a",
            1,
            List.of(new DatasetPartitionMetadata(startTime + 5, startTime + 6, List.of("a", "b"))),
            "fooService");

    datasetMetadataStore.createSync(serviceWithDataInPartitionA);

    await().until(() -> datasetMetadataStore.listSync().size() == 1);
    await().until(() -> snapshotMetadataStore.listSync().size() == 3);

    replicaRestoreService.startAsync();
    replicaRestoreService.awaitRunning();

    managerApiStub.restoreReplica(
        ManagerApi.RestoreReplicaRequest.newBuilder()
            .setServiceName("foo")
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    await().until(() -> replicaMetadataStore.listSync().size() == 2);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(2);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(0);

    replicaRestoreService.stopAsync();
  }

  @Test
  public void shouldRestoreGivenSnapshotIds() {
    long startTime = Instant.now().toEpochMilli();

    SnapshotMetadata snapshotFoo =
        new SnapshotMetadata("foo", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata snapshotBar =
        new SnapshotMetadata("bar", startTime + 10, startTime + 15, 0, "b", 0);
    SnapshotMetadata snapshotBaz =
        new SnapshotMetadata("baz", startTime + 10, startTime + 15, 0, "c", 0);

    snapshotMetadataStore.createSync(snapshotFoo);
    snapshotMetadataStore.createSync(snapshotBar);
    snapshotMetadataStore.createSync(snapshotBaz);
    await().until(() -> snapshotMetadataStore.listSync().size() == 3);

    replicaRestoreService.startAsync();
    replicaRestoreService.awaitRunning();

    managerApiStub.restoreReplicaIds(
        ManagerApi.RestoreReplicaIdsRequest.newBuilder()
            .addAllIdsToRestore(List.of("foo", "bar", "baz"))
            .build());

    await().until(() -> replicaMetadataStore.listSync().size() == 3);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(3);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(0);

    replicaRestoreService.stopAsync();
  }

  @Test
  public void shouldCreateAndGetNewFieldRedaction() {
    String redactionName = "testRedaction";
    String fieldName = "testfieldName";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    managerApiStub.createFieldRedaction(
        ManagerApi.CreateFieldRedactionRequest.newBuilder()
            .setName(redactionName)
            .setFieldName(fieldName)
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    Metadata.RedactedFieldMetadata getRedactedFieldResponse =
        managerApiStub.getFieldRedaction(
            ManagerApi.GetFieldRedactionRequest.newBuilder().setName(redactionName).build());
    assertThat(getRedactedFieldResponse.getName()).isEqualTo(redactionName);
    assertThat(getRedactedFieldResponse.getFieldName()).isEqualTo(fieldName);
    assertThat(getRedactedFieldResponse.getStartTimeEpochMs()).isEqualTo(start);
    assertThat(getRedactedFieldResponse.getEndTimeEpochMs()).isEqualTo(end);

    FieldRedactionMetadata fieldRedactionMetadata =
        fieldRedactionMetadataStore.getSync(redactionName);
    assertThat(fieldRedactionMetadata.getName()).isEqualTo(redactionName);
    assertThat(fieldRedactionMetadata.getFieldName()).isEqualTo(fieldName);
    assertThat(fieldRedactionMetadata.getStartTimeEpochMs()).isEqualTo(start);
    assertThat(fieldRedactionMetadata.getEndTimeEpochMs()).isEqualTo(end);
  }

  @Test
  public void shouldListExistingFieldRedactions() {
    String redactionName1 = "testFieldRedaction1";
    String field1 = "testField1";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    managerApiStub.createFieldRedaction(
        ManagerApi.CreateFieldRedactionRequest.newBuilder()
            .setName(redactionName1)
            .setFieldName(field1)
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    String redactionName2 = "testFieldRedaction2";
    String field2 = "testField2";

    managerApiStub.createFieldRedaction(
        ManagerApi.CreateFieldRedactionRequest.newBuilder()
            .setName(redactionName2)
            .setFieldName(field2)
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    ManagerApi.ListFieldRedactionsResponse listFieldRedactionsResponse =
        managerApiStub.listFieldRedactions(
            ManagerApi.ListFieldRedactionsRequest.newBuilder().build());

    assertThat(
        listFieldRedactionsResponse
            .getRedactedFieldsList()
            .containsAll(
                List.of(
                    Metadata.RedactedFieldMetadata.newBuilder()
                        .setName(redactionName1)
                        .setFieldName(field1)
                        .setStartTimeEpochMs(start)
                        .setEndTimeEpochMs(end)
                        .build(),
                    Metadata.RedactedFieldMetadata.newBuilder()
                        .setName(redactionName2)
                        .setFieldName(field2)
                        .setStartTimeEpochMs(start)
                        .setEndTimeEpochMs(end)
                        .build())));

    assertThat(AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size())
        .isEqualTo(2);
    assertThat(
        AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore)
            .containsAll(
                List.of(
                    new FieldRedactionMetadata(redactionName1, field1, start, end),
                    new FieldRedactionMetadata(redactionName2, field2, start, end))));
  }

  @Test
  public void shouldDeleteExistingFieldRedaction() {
    String redactionName = "testRedaction";
    String fieldName = "testFieldName";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    managerApiStub.createFieldRedaction(
        ManagerApi.CreateFieldRedactionRequest.newBuilder()
            .setName(redactionName)
            .setFieldName(fieldName)
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    Metadata.RedactedFieldMetadata deleteRedactedFieldResponse =
        managerApiStub.deleteFieldRedaction(
            ManagerApi.DeleteFieldRedactionRequest.newBuilder().setName(redactionName).build());
    assertThat(deleteRedactedFieldResponse.getName()).isEqualTo(redactionName);
    assertThat(deleteRedactedFieldResponse.getFieldName()).isEqualTo(fieldName);
    assertThat(deleteRedactedFieldResponse.getStartTimeEpochMs()).isEqualTo(start);
    assertThat(deleteRedactedFieldResponse.getEndTimeEpochMs()).isEqualTo(end);

    assertThat(fieldRedactionMetadataStore.hasSync(redactionName)).isFalse();
  }

  @Test
  public void shouldErrorCreatingDuplicateFieldRedactionName() {
    String redactionName = "testFieldRedaction";
    String fieldName1 = "fieldName1";
    String fieldName2 = "fieldName2";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    managerApiStub.createFieldRedaction(
        ManagerApi.CreateFieldRedactionRequest.newBuilder()
            .setName(redactionName)
            .setFieldName(fieldName1)
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createFieldRedaction(
                        ManagerApi.CreateFieldRedactionRequest.newBuilder()
                            .setName(redactionName)
                            .setFieldName(fieldName2)
                            .setStartTimeEpochMs(start)
                            .setEndTimeEpochMs(end)
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    FieldRedactionMetadata fieldRedactionMetadata =
        fieldRedactionMetadataStore.getSync(redactionName);
    assertThat(fieldRedactionMetadata.getName()).isEqualTo(redactionName);
    assertThat(fieldRedactionMetadata.getFieldName()).isEqualTo(fieldName1);
    assertThat(fieldRedactionMetadata.getStartTimeEpochMs()).isEqualTo(start);
    assertThat(fieldRedactionMetadata.getEndTimeEpochMs()).isEqualTo(end);
  }

  @Test
  public void shouldErrorCreatingWithInvalidRedactionNames() {
    String fieldName = "testFieldName";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createFieldRedaction(
                        ManagerApi.CreateFieldRedactionRequest.newBuilder()
                            .setName("")
                            .setFieldName(fieldName)
                            .setStartTimeEpochMs(start)
                            .setEndTimeEpochMs(end)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable1.getStatus().getDescription()).isEqualTo("name can't be null or empty.");

    StatusRuntimeException throwable2 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createFieldRedaction(
                        ManagerApi.CreateFieldRedactionRequest.newBuilder()
                            .setName("/")
                            .setFieldName(fieldName)
                            .setStartTimeEpochMs(start)
                            .setEndTimeEpochMs(end)
                            .build()));
    assertThat(throwable2.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    StatusRuntimeException throwable3 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createFieldRedaction(
                        ManagerApi.CreateFieldRedactionRequest.newBuilder()
                            .setName(".")
                            .setFieldName(fieldName)
                            .setStartTimeEpochMs(start)
                            .setEndTimeEpochMs(end)
                            .build()));
    assertThat(throwable3.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size())
        .isEqualTo(0);
  }

  @Test
  public void shouldErrorWithEmptyFieldName() {
    String redactionName = "testRedaction";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createFieldRedaction(
                        ManagerApi.CreateFieldRedactionRequest.newBuilder()
                            .setName(redactionName)
                            .setFieldName("")
                            .setStartTimeEpochMs(start)
                            .setEndTimeEpochMs(end)
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable.getStatus().getDescription()).isEqualTo("field name cannot be null");

    assertThat(AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size())
        .isEqualTo(0);
  }
}
