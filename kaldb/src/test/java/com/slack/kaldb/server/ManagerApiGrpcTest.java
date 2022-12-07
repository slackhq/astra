package com.slack.kaldb.server;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.server.ManagerApiGrpc.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.kaldb.clusterManager.ReplicaRestoreService;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.InternalMetadataStoreException;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.manager_api.ManagerApi;
import com.slack.kaldb.proto.manager_api.ManagerApiServiceGrpc;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.testlib.MetricsUtil;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ManagerApiGrpcTest {
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private MetadataStore metadataStore;
  private DatasetMetadataStore datasetMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;
  private ReplicaRestoreService replicaRestoreService;
  private ManagerApiServiceGrpc.ManagerApiServiceBlockingStub managerApiStub;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("ManagerApiGrpcTest")
            .setZkSessionTimeoutMs(30000)
            .setZkConnectionTimeoutMs(30000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);
    datasetMetadataStore = spy(new DatasetMetadataStore(metadataStore, true));
    snapshotMetadataStore = spy(new SnapshotMetadataStore(metadataStore, true));
    replicaMetadataStore = spy(new ReplicaMetadataStore(metadataStore, true));

    KaldbConfigs.ManagerConfig.ReplicaRestoreServiceConfig replicaRecreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaRestoreServiceConfig.newBuilder()
            .setMaxReplicasPerRequest(200)
            .setReplicaLifespanMins(60)
            .setSchedulePeriodMins(30)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaRestoreServiceConfig(replicaRecreationServiceConfig)
            .build();

    replicaRestoreService =
        new ReplicaRestoreService(replicaMetadataStore, meterRegistry, managerConfig);

    grpcCleanup.register(
        InProcessServerBuilder.forName(this.getClass().toString())
            .directExecutor()
            .addService(
                new ManagerApiGrpc(
                    datasetMetadataStore, snapshotMetadataStore, replicaRestoreService))
            .build()
            .start());
    ManagedChannel channel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName(this.getClass().toString()).directExecutor().build());

    managerApiStub = ManagerApiServiceGrpc.newBlockingStub(channel);
  }

  @After
  public void tearDown() throws Exception {
    replicaRestoreService.stopAsync();
    replicaRestoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);

    replicaMetadataStore.close();
    snapshotMetadataStore.close();
    datasetMetadataStore.close();
    metadataStore.close();

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

    DatasetMetadata datasetMetadata = datasetMetadataStore.getNodeSync(datasetName);
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

    DatasetMetadata datasetMetadata = datasetMetadataStore.getNodeSync(datasetName);
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

    assertThat(datasetMetadataStore.listSync().size()).isEqualTo(0);
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

    assertThat(datasetMetadataStore.listSync().size()).isEqualTo(0);
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

    DatasetMetadata datasetMetadata = datasetMetadataStore.getNodeSync(datasetName);
    assertThat(datasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.getServiceNamePattern()).isEqualTo(serviceNamePattern);
    assertThat(datasetMetadata.getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(datasetMetadata.getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.getPartitionConfigs().size()).isEqualTo(0);

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

    datasetMetadata = datasetMetadataStore.getNodeSync(datasetName);
    assertThat(datasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.getServiceNamePattern()).isEqualTo(updatedServiceNamePattern);
    assertThat(datasetMetadata.getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(datasetMetadata.getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.getPartitionConfigs().size()).isEqualTo(0);
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

    assertThat(datasetMetadataStore.listSync().size()).isEqualTo(0);
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

    DatasetMetadata firstDatasetMetadata = datasetMetadataStore.getNodeSync(datasetName);
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
    Metadata.DatasetMetadata secondAssignment =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());

    assertThat(secondAssignment.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(secondAssignment.getPartitionConfigsList().size()).isEqualTo(2);
    assertThat(secondAssignment.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(secondAssignment.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(secondAssignment.getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(secondAssignment.getPartitionConfigsList().get(1).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(secondAssignment.getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    DatasetMetadata secondDatasetMetadata = datasetMetadataStore.getNodeSync(datasetName);
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
    Metadata.DatasetMetadata thirdAssignment =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());

    assertThat(thirdAssignment.getThroughputBytes()).isEqualTo(updatedThroughputBytes);
    assertThat(thirdAssignment.getPartitionConfigsList().size()).isEqualTo(2);
    assertThat(thirdAssignment.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(thirdAssignment.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(thirdAssignment.getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(thirdAssignment.getPartitionConfigsList().get(1).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(thirdAssignment.getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    DatasetMetadata thirdDatasetMetadata = datasetMetadataStore.getNodeSync(datasetName);
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

    assertThat(datasetMetadataStore.listSync().size()).isEqualTo(0);
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

    assertThat(datasetMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(
        datasetMetadataStore
            .listSync()
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

    assertThat(datasetMetadataStore.listSync().size()).isEqualTo(0);
  }

  @Test
  public void shouldFetchSnapshotsWithinTimeframeAndPartition() {
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    SnapshotMetadata overlapsStartTimeIncluded =
        new SnapshotMetadata(
            "a", "a", startTime, startTime + 6, 0, "a", Metadata.IndexType.LOGS_LUCENE9);
    SnapshotMetadata overlapsStartTimeExcluded =
        new SnapshotMetadata(
            "b", "b", startTime, startTime + 6, 0, "b", Metadata.IndexType.LOGS_LUCENE9);

    SnapshotMetadata fullyOverlapsStartEndTimeIncluded =
        new SnapshotMetadata(
            "c", "c", startTime + 4, startTime + 11, 0, "a", Metadata.IndexType.LOGS_LUCENE9);
    SnapshotMetadata fullyOverlapsStartEndTimeExcluded =
        new SnapshotMetadata(
            "d", "d", startTime + 4, startTime + 11, 0, "b", Metadata.IndexType.LOGS_LUCENE9);

    SnapshotMetadata partiallyOverlapsStartEndTimeIncluded =
        new SnapshotMetadata(
            "e", "e", startTime + 4, startTime + 5, 0, "a", Metadata.IndexType.LOGS_LUCENE9);
    SnapshotMetadata partiallyOverlapsStartEndTimeExcluded =
        new SnapshotMetadata(
            "f", "f", startTime + 4, startTime + 5, 0, "b", Metadata.IndexType.LOGS_LUCENE9);

    SnapshotMetadata overlapsEndTimeIncluded =
        new SnapshotMetadata(
            "g", "g", startTime + 10, startTime + 15, 0, "a", Metadata.IndexType.LOGS_LUCENE9);
    SnapshotMetadata overlapsEndTimeExcluded =
        new SnapshotMetadata(
            "h", "h", startTime + 10, startTime + 15, 0, "b", Metadata.IndexType.LOGS_LUCENE9);

    SnapshotMetadata notWithinStartEndTimeExcluded1 =
        new SnapshotMetadata(
            "i", "i", startTime, startTime + 4, 0, "a", Metadata.IndexType.LOGS_LUCENE9);
    SnapshotMetadata notWithinStartEndTimeExcluded2 =
        new SnapshotMetadata(
            "j", "j", startTime + 11, startTime + 15, 0, "a", Metadata.IndexType.LOGS_LUCENE9);

    DatasetMetadata datasetWithDataInPartitionA =
        new DatasetMetadata(
            "foo",
            "a",
            1,
            List.of(new DatasetPartitionMetadata(startTime + 5, startTime + 6, List.of("a"))),
            "fooService");

    datasetMetadataStore.createSync(datasetWithDataInPartitionA);

    await().until(() -> datasetMetadataStore.getCached().size() == 1);

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
        new SnapshotMetadata(
            "g", "g", startTime + 10, startTime + 15, 0, "a", Metadata.IndexType.LOGS_LUCENE9);
    SnapshotMetadata snapshotExcluded =
        new SnapshotMetadata(
            "h", "h", startTime + 10, startTime + 15, 0, "b", Metadata.IndexType.LOGS_LUCENE9);

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

    await().until(() -> datasetMetadataStore.getCached().size() == 1);
    await().until(() -> snapshotMetadataStore.getCached().size() == 2);

    managerApiStub.restoreReplica(
        ManagerApi.RestoreReplicaRequest.newBuilder()
            .setServiceName("foo")
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    await().until(() -> replicaMetadataStore.getCached().size() == 1);
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
        new SnapshotMetadata(
            "a", "a", startTime + 10, startTime + 15, 0, "a", Metadata.IndexType.LOGS_LUCENE9);
    SnapshotMetadata snapshotIncluded2 =
        new SnapshotMetadata(
            "b", "b", startTime + 10, startTime + 15, 0, "b", Metadata.IndexType.LOGS_LUCENE9);
    SnapshotMetadata snapshotExcluded =
        new SnapshotMetadata(
            "c", "c", startTime + 10, startTime + 15, 0, "c", Metadata.IndexType.LOGS_LUCENE9);

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

    await().until(() -> datasetMetadataStore.getCached().size() == 1);
    await().until(() -> snapshotMetadataStore.getCached().size() == 3);

    replicaRestoreService.startAsync();
    replicaRestoreService.awaitRunning();

    managerApiStub.restoreReplica(
        ManagerApi.RestoreReplicaRequest.newBuilder()
            .setServiceName("foo")
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    await().until(() -> replicaMetadataStore.getCached().size() == 2);
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
        new SnapshotMetadata(
            "foo", "a", startTime + 10, startTime + 15, 0, "a", Metadata.IndexType.LOGS_LUCENE9);
    SnapshotMetadata snapshotBar =
        new SnapshotMetadata(
            "bar", "b", startTime + 10, startTime + 15, 0, "b", Metadata.IndexType.LOGS_LUCENE9);
    SnapshotMetadata snapshotBaz =
        new SnapshotMetadata(
            "baz", "c", startTime + 10, startTime + 15, 0, "c", Metadata.IndexType.LOGS_LUCENE9);

    snapshotMetadataStore.createSync(snapshotFoo);
    snapshotMetadataStore.createSync(snapshotBar);
    snapshotMetadataStore.createSync(snapshotBaz);
    await().until(() -> snapshotMetadataStore.getCached().size() == 3);

    replicaRestoreService.startAsync();
    replicaRestoreService.awaitRunning();

    managerApiStub.restoreReplicaIds(
        ManagerApi.RestoreReplicaIdsRequest.newBuilder()
            .addAllIdsToRestore(List.of("foo", "bar", "baz"))
            .build());

    await().until(() -> replicaMetadataStore.getCached().size() == 3);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(3);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(0);

    replicaRestoreService.stopAsync();
  }
}
