package com.slack.kaldb.server;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.kaldb.metadata.service.ServiceMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.manager_api.ManagerApi;
import com.slack.kaldb.proto.manager_api.ManagerApiServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
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
    long serviceBytes = 1;

    managerApiStub.createService(
        ManagerApi.CreateServiceRequest.newBuilder()
            .setName(serviceName)
            .setOwner(serviceOwner)
            .setThroughputBytes(serviceBytes)
            .build());

    ManagerApi.GetServiceResponse getServiceResponse =
        managerApiStub.getService(
            ManagerApi.GetServiceRequest.newBuilder().setName(serviceName).build());
    assertThat(getServiceResponse.getName()).isEqualTo(serviceName);
    assertThat(getServiceResponse.getOwner()).isEqualTo(serviceOwner);
    assertThat(getServiceResponse.getThroughputBytes()).isEqualTo(serviceBytes);
  }

  @Test
  public void shouldErrorCreatingDuplicateServiceName() {
    String serviceName = "testService";
    String serviceOwner = "testOwner";
    long serviceBytes = 1;

    managerApiStub.createService(
        ManagerApi.CreateServiceRequest.newBuilder()
            .setName(serviceName)
            .setOwner(serviceOwner)
            .setThroughputBytes(serviceBytes)
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createService(
                        ManagerApi.CreateServiceRequest.newBuilder()
                            .setName(serviceName)
                            .setOwner(serviceOwner)
                            .setThroughputBytes(serviceBytes)
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
  }

  @Test
  public void shouldErrorCreatingWithInvalidServiceNames() {
    String serviceOwner = "testOwner";
    long serviceBytes = 1;

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createService(
                        ManagerApi.CreateServiceRequest.newBuilder()
                            .setName("")
                            .setOwner(serviceOwner)
                            .setThroughputBytes(serviceBytes)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable1.getStatus().getDescription()).isEqualTo("name can't be null or empty.");

    StatusRuntimeException throwable2 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createService(
                        ManagerApi.CreateServiceRequest.newBuilder()
                            .setName("/")
                            .setOwner(serviceOwner)
                            .setThroughputBytes(serviceBytes)
                            .build()));
    assertThat(throwable2.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    StatusRuntimeException throwable3 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createService(
                        ManagerApi.CreateServiceRequest.newBuilder()
                            .setName(".")
                            .setOwner(serviceOwner)
                            .setThroughputBytes(serviceBytes)
                            .build()));
    assertThat(throwable3.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
  }

  @Test
  public void shouldErrorWithEmptyOwnerInformation() {
    String serviceName = "testService";
    long serviceBytes = 1;

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createService(
                        ManagerApi.CreateServiceRequest.newBuilder()
                            .setName(serviceName)
                            .setOwner("")
                            .setThroughputBytes(serviceBytes)
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable.getStatus().getDescription()).isEqualTo("owner must not be null or blank");
  }

  @Test
  public void shouldErrorCreatingWithInvalidThroughput() {
    String serviceName = "testService";
    String serviceOwner = "testOwner";

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createService(
                        ManagerApi.CreateServiceRequest.newBuilder()
                            .setName(serviceName)
                            .setOwner(serviceOwner)
                            .setThroughputBytes(0)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable1.getStatus().getDescription())
        .isEqualTo("throughputBytes must be greater than 0");

    StatusRuntimeException throwable2 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createService(
                        ManagerApi.CreateServiceRequest.newBuilder()
                            .setName(serviceName)
                            .setOwner(serviceOwner)
                            .setThroughputBytes(-1)
                            .build()));
    assertThat(throwable2.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable2.getStatus().getDescription())
        .isEqualTo("throughputBytes must be greater than 0");
  }

  @Test
  public void shouldUpdateExistingService() {
    String serviceName = "testService";
    String serviceOwner = "testOwner";
    long serviceBytes = 1;

    managerApiStub.createService(
        ManagerApi.CreateServiceRequest.newBuilder()
            .setName(serviceName)
            .setOwner(serviceOwner)
            .setThroughputBytes(serviceBytes)
            .build());

    String updatedServiceOwner = "testOwnerUpdated";
    ManagerApi.UpdateServiceResponse updatedServiceResponse =
        managerApiStub.updateService(
            ManagerApi.UpdateServiceRequest.newBuilder()
                .setName(serviceName)
                .setOwner(updatedServiceOwner)
                .setThroughputBytes(serviceBytes)
                .build());

    assertThat(updatedServiceResponse.getName()).isEqualTo(serviceName);
    assertThat(updatedServiceResponse.getOwner()).isEqualTo(updatedServiceOwner);
    assertThat(updatedServiceResponse.getThroughputBytes()).isEqualTo(serviceBytes);
  }

  @Test
  public void shouldErrorUpdatingWithInvalidThroughput() {
    String serviceName = "testService";
    String serviceOwner = "testOwner";
    long serviceBytes = 1;

    managerApiStub.createService(
        ManagerApi.CreateServiceRequest.newBuilder()
            .setName(serviceName)
            .setOwner(serviceOwner)
            .setThroughputBytes(serviceBytes)
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updateService(
                        ManagerApi.UpdateServiceRequest.newBuilder()
                            .setName(serviceName)
                            .setOwner(serviceOwner)
                            .setThroughputBytes(0)
                            .build()));
    Status status = throwable.getStatus();

    assertThat(status.getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(status.getDescription()).isEqualTo("throughputBytes must be greater than 0");
  }

  // todo
  public void shouldErrorGettingNonexistentService() {}

  // todo
  public void shouldAppendServicePartitions() {}

  // todo
  public void shouldErrorWithInvalidPartitionAssignment() {}

  // todo
  public void shouldErrorAppendingPartitionsNonexistentService() {}

  // todo
  public void shouldListExistingPartitions() {}

  // todo
  public void shouldErrorListingPartitionsNonexistentService() {}

  // todo
  public void shouldListExistingServices() {}
}
