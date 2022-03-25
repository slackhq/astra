package com.slack.kaldb.server;

import static com.slack.kaldb.server.ManagerApiGrpc.MAX_TIME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.kaldb.metadata.service.ServiceMetadata;
import com.slack.kaldb.metadata.service.ServiceMetadataStore;
import com.slack.kaldb.metadata.zookeeper.InternalMetadataStoreException;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.manager_api.ManagerApi;
import com.slack.kaldb.proto.manager_api.ManagerApiServiceGrpc;
import com.slack.kaldb.proto.metadata.Metadata;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
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
  private ServiceMetadataStore serviceMetadataStore;
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
    serviceMetadataStore = spy(new ServiceMetadataStore(metadataStore, true));

    grpcCleanup.register(
        InProcessServerBuilder.forName(this.getClass().toString())
            .directExecutor()
            .addService(new ManagerApiGrpc(serviceMetadataStore))
            .build()
            .start());
    ManagedChannel channel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName(this.getClass().toString()).directExecutor().build());

    managerApiStub = ManagerApiServiceGrpc.newBlockingStub(channel);
  }

  @After
  public void tearDown() throws Exception {
    serviceMetadataStore.close();
    metadataStore.close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldCreateAndGetNewService() {
    String serviceName = "testService";
    String serviceOwner = "testOwner";

    managerApiStub.createServiceMetadata(
        ManagerApi.CreateServiceMetadataRequest.newBuilder()
            .setName(serviceName)
            .setOwner(serviceOwner)
            .build());

    Metadata.ServiceMetadata getServiceMetadataResponse =
        managerApiStub.getServiceMetadata(
            ManagerApi.GetServiceMetadataRequest.newBuilder().setName(serviceName).build());
    assertThat(getServiceMetadataResponse.getName()).isEqualTo(serviceName);
    assertThat(getServiceMetadataResponse.getOwner()).isEqualTo(serviceOwner);
    assertThat(getServiceMetadataResponse.getThroughputBytes()).isEqualTo(0);
    assertThat(getServiceMetadataResponse.getPartitionConfigsList().size()).isEqualTo(0);

    ServiceMetadata serviceMetadata = serviceMetadataStore.getNodeSync(serviceName);
    assertThat(serviceMetadata.getName()).isEqualTo(serviceName);
    assertThat(serviceMetadata.getOwner()).isEqualTo(serviceOwner);
    assertThat(serviceMetadata.getThroughputBytes()).isEqualTo(0);
    assertThat(serviceMetadata.getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorCreatingDuplicateServiceName() {
    String serviceName = "testService";
    String serviceOwner1 = "testOwner1";
    String serviceOwner2 = "testOwner2";

    managerApiStub.createServiceMetadata(
        ManagerApi.CreateServiceMetadataRequest.newBuilder()
            .setName(serviceName)
            .setOwner(serviceOwner1)
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createServiceMetadata(
                        ManagerApi.CreateServiceMetadataRequest.newBuilder()
                            .setName(serviceName)
                            .setOwner(serviceOwner2)
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    ServiceMetadata serviceMetadata = serviceMetadataStore.getNodeSync(serviceName);
    assertThat(serviceMetadata.getName()).isEqualTo(serviceName);
    assertThat(serviceMetadata.getOwner()).isEqualTo(serviceOwner1);
    assertThat(serviceMetadata.getThroughputBytes()).isEqualTo(0);
    assertThat(serviceMetadata.getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorCreatingWithInvalidServiceNames() {
    String serviceOwner = "testOwner";

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createServiceMetadata(
                        ManagerApi.CreateServiceMetadataRequest.newBuilder()
                            .setName("")
                            .setOwner(serviceOwner)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable1.getStatus().getDescription()).isEqualTo("name can't be null or empty.");

    StatusRuntimeException throwable2 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createServiceMetadata(
                        ManagerApi.CreateServiceMetadataRequest.newBuilder()
                            .setName("/")
                            .setOwner(serviceOwner)
                            .build()));
    assertThat(throwable2.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    StatusRuntimeException throwable3 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createServiceMetadata(
                        ManagerApi.CreateServiceMetadataRequest.newBuilder()
                            .setName(".")
                            .setOwner(serviceOwner)
                            .build()));
    assertThat(throwable3.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(serviceMetadataStore.listSync().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorWithEmptyOwnerInformation() {
    String serviceName = "testService";

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createServiceMetadata(
                        ManagerApi.CreateServiceMetadataRequest.newBuilder()
                            .setName(serviceName)
                            .setOwner("")
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable.getStatus().getDescription()).isEqualTo("owner must not be null or blank");

    assertThat(serviceMetadataStore.listSync().size()).isEqualTo(0);
  }

  @Test
  public void shouldUpdateExistingService() {
    String serviceName = "testService";
    String serviceOwner = "testOwner";

    managerApiStub.createServiceMetadata(
        ManagerApi.CreateServiceMetadataRequest.newBuilder()
            .setName(serviceName)
            .setOwner(serviceOwner)
            .build());

    String updatedServiceOwner = "testOwnerUpdated";
    Metadata.ServiceMetadata updatedServiceResponse =
        managerApiStub.updateServiceMetadata(
            ManagerApi.UpdateServiceMetadataRequest.newBuilder()
                .setName(serviceName)
                .setOwner(updatedServiceOwner)
                .build());

    assertThat(updatedServiceResponse.getName()).isEqualTo(serviceName);
    assertThat(updatedServiceResponse.getOwner()).isEqualTo(updatedServiceOwner);
    assertThat(updatedServiceResponse.getThroughputBytes()).isEqualTo(0);
    assertThat(updatedServiceResponse.getPartitionConfigsList().size()).isEqualTo(0);

    ServiceMetadata serviceMetadata = serviceMetadataStore.getNodeSync(serviceName);
    assertThat(serviceMetadata.getName()).isEqualTo(serviceName);
    assertThat(serviceMetadata.getOwner()).isEqualTo(updatedServiceOwner);
    assertThat(serviceMetadata.getThroughputBytes()).isEqualTo(0);
    assertThat(serviceMetadata.getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorGettingNonexistentService() {
    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.getServiceMetadata(
                        ManagerApi.GetServiceMetadataRequest.newBuilder().setName("foo").build()));
    Status status = throwable.getStatus();
    assertThat(status.getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(serviceMetadataStore.listSync().size()).isEqualTo(0);
  }

  @Test
  public void shouldUpdatePartitionAssignments() {
    String serviceName = "testService";
    String serviceOwner = "testOwner";

    Metadata.ServiceMetadata initialServiceRequest =
        managerApiStub.createServiceMetadata(
            ManagerApi.CreateServiceMetadataRequest.newBuilder()
                .setName(serviceName)
                .setOwner(serviceOwner)
                .build());
    assertThat(initialServiceRequest.getPartitionConfigsList().size()).isEqualTo(0);

    long nowMs = Instant.now().toEpochMilli();
    long throughputBytes = 10;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(serviceName)
            .setThroughputBytes(throughputBytes)
            .addAllPartitionIds(List.of("1", "2"))
            .build());
    Metadata.ServiceMetadata firstAssignment =
        managerApiStub.getServiceMetadata(
            ManagerApi.GetServiceMetadataRequest.newBuilder().setName(serviceName).build());

    assertThat(firstAssignment.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstAssignment.getPartitionConfigsList().size()).isEqualTo(1);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    ServiceMetadata firstServiceMetadata = serviceMetadataStore.getNodeSync(serviceName);
    assertThat(firstServiceMetadata.getName()).isEqualTo(serviceName);
    assertThat(firstServiceMetadata.getOwner()).isEqualTo(serviceOwner);
    assertThat(firstServiceMetadata.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstServiceMetadata.getPartitionConfigs().size()).isEqualTo(1);

    // only update the partition assignment, leaving throughput
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(serviceName)
            .setThroughputBytes(-1)
            .addAllPartitionIds(List.of("3", "4", "5"))
            .build());
    Metadata.ServiceMetadata secondAssignment =
        managerApiStub.getServiceMetadata(
            ManagerApi.GetServiceMetadataRequest.newBuilder().setName(serviceName).build());

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

    ServiceMetadata secondServiceMetadata = serviceMetadataStore.getNodeSync(serviceName);
    assertThat(secondServiceMetadata.getName()).isEqualTo(serviceName);
    assertThat(secondServiceMetadata.getOwner()).isEqualTo(serviceOwner);
    assertThat(secondServiceMetadata.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(secondServiceMetadata.getPartitionConfigs().size()).isEqualTo(2);

    // only update the throughput, leaving the partition assignments
    long updatedThroughputBytes = 12;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(serviceName)
            .setThroughputBytes(updatedThroughputBytes)
            .build());
    Metadata.ServiceMetadata thirdAssignment =
        managerApiStub.getServiceMetadata(
            ManagerApi.GetServiceMetadataRequest.newBuilder().setName(serviceName).build());

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

    ServiceMetadata thirdServiceMetadata = serviceMetadataStore.getNodeSync(serviceName);
    assertThat(thirdServiceMetadata.getName()).isEqualTo(serviceName);
    assertThat(thirdServiceMetadata.getOwner()).isEqualTo(serviceOwner);
    assertThat(thirdServiceMetadata.getThroughputBytes()).isEqualTo(updatedThroughputBytes);
    assertThat(thirdServiceMetadata.getPartitionConfigs().size()).isEqualTo(2);
  }

  @Test
  public void shouldErrorUpdatingPartitionAssignmentNonexistentService() {
    String serviceName = "testService";
    List<String> partitionList = List.of("1", "2");

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(serviceName)
                            .setThroughputBytes(-1)
                            .addAllPartitionIds(partitionList)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(serviceMetadataStore.listSync().size()).isEqualTo(0);
  }

  @Test
  public void shouldListExistingServices() {
    String serviceName1 = "testService1";
    String serviceOwner1 = "testOwner1";

    managerApiStub.createServiceMetadata(
        ManagerApi.CreateServiceMetadataRequest.newBuilder()
            .setName(serviceName1)
            .setOwner(serviceOwner1)
            .build());

    String serviceName2 = "testService2";
    String serviceOwner2 = "testOwner2";

    managerApiStub.createServiceMetadata(
        ManagerApi.CreateServiceMetadataRequest.newBuilder()
            .setName(serviceName2)
            .setOwner(serviceOwner2)
            .build());

    ManagerApi.ListServiceMetadataResponse listServiceMetadataResponse =
        managerApiStub.listServiceMetadata(
            ManagerApi.ListServiceMetadataRequest.newBuilder().build());

    assertThat(
        listServiceMetadataResponse
            .getServiceMetadataList()
            .containsAll(
                List.of(
                    Metadata.ServiceMetadata.newBuilder()
                        .setName(serviceName1)
                        .setOwner(serviceOwner1)
                        .setThroughputBytes(0)
                        .build(),
                    Metadata.ServiceMetadata.newBuilder()
                        .setName(serviceName2)
                        .setOwner(serviceOwner2)
                        .setThroughputBytes(0)
                        .build())));

    assertThat(serviceMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(
        serviceMetadataStore
            .listSync()
            .containsAll(
                List.of(
                    new ServiceMetadata(serviceName1, serviceOwner1, 0, Collections.emptyList()),
                    new ServiceMetadata(serviceName2, serviceOwner2, 0, Collections.emptyList()))));
  }

  @Test
  public void shouldHandleZkErrorsGracefully() {
    String serviceName = "testZkErrorsService";
    String serviceOwner = "testZkErrorsOwner";
    String errorString = "zkError";

    doThrow(new InternalMetadataStoreException(errorString))
        .when(serviceMetadataStore)
        .createSync(
            eq(new ServiceMetadata(serviceName, serviceOwner, 0L, Collections.emptyList())));

    StatusRuntimeException throwableCreate =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createServiceMetadata(
                        ManagerApi.CreateServiceMetadataRequest.newBuilder()
                            .setName(serviceName)
                            .setOwner(serviceOwner)
                            .build()));

    assertThat(throwableCreate.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwableCreate.getStatus().getDescription()).isEqualTo(errorString);

    doThrow(new InternalMetadataStoreException(errorString))
        .when(serviceMetadataStore)
        .updateSync(
            eq(new ServiceMetadata(serviceName, serviceOwner, 0L, Collections.emptyList())));

    StatusRuntimeException throwableUpdate =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updateServiceMetadata(
                        ManagerApi.UpdateServiceMetadataRequest.newBuilder()
                            .setName(serviceName)
                            .setOwner(serviceOwner)
                            .build()));

    assertThat(throwableUpdate.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwableUpdate.getStatus().getDescription()).contains(serviceName);

    assertThat(serviceMetadataStore.listSync().size()).isEqualTo(0);
  }
}
