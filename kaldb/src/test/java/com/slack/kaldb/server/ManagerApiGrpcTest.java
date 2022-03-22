package com.slack.kaldb.server;

import brave.Tracing;
import com.slack.kaldb.metadata.service.ServiceMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.manager_api.ManagerApi;
import com.slack.kaldb.proto.manager_api.ManagerApiServiceGrpc;
import io.grpc.ManagedChannel;
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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.spy;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ManagerApiGrpcTest {
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

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
        grpcCleanup.register(InProcessChannelBuilder.forName(this.getClass().toString())
            .directExecutor()
            .build());

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

    managerApiStub.createService(ManagerApi.CreateServiceRequest.newBuilder()
            .setName(serviceName)
            .setOwner(serviceOwner)
            .setThroughputBytes(serviceBytes)
        .build());

    ManagerApi.GetServiceResponse getServiceResponse = managerApiStub.getService(ManagerApi.GetServiceRequest.newBuilder()
            .setName(serviceName)
        .build());
    assertThat(getServiceResponse.getName()).isEqualTo(serviceName);
    assertThat(getServiceResponse.getOwner()).isEqualTo(serviceOwner);
    assertThat(getServiceResponse.getThroughputBytes()).isEqualTo(serviceBytes);
  }

  @Test
  public void shouldErrorCreatingDuplicateServiceName() {
    String serviceName = "testService";
    String serviceOwner = "testOwner";
    long serviceBytes = 1;

    managerApiStub.createService(ManagerApi.CreateServiceRequest.newBuilder()
        .setName(serviceName)
        .setOwner(serviceOwner)
        .setThroughputBytes(serviceBytes)
        .build());

    assertThatThrownBy(() -> managerApiStub.createService(ManagerApi.CreateServiceRequest.newBuilder()
        .setName(serviceName)
        .setOwner(serviceOwner)
        .setThroughputBytes(serviceBytes)
        .build()));
  }

  public void shouldErrorCreatingWithEmptyServiceName() {
  }

  public void shouldErrorCreatingWithInvalidThroughput() {
  }

  public void shouldUpdateExistingService() {
  }

  public void shouldErrorUpdatingWithInvalidThroughput() {
  }

  public void shouldErrorGettingNonexistentService() {
  }

  public void shouldAppendServicePartitions() {
  }

  public void shouldErrorWithInvalidPartitionAssignment() {
  }

  public void shouldErrorAppendingPartitionsNonexistentService() {
  }

  public void shouldListExistingPartitions() {
  }

  public void shouldErrorListingPartitionsNonexistentService() {
  }

  public void shouldListExistingServices() {
  }
}
