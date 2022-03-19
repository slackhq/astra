package com.slack.kaldb.server;

import com.google.common.collect.ImmutableList;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.slack.kaldb.metadata.service.ServiceMetadata;
import com.slack.kaldb.metadata.service.ServiceMetadataStore;
import com.slack.kaldb.metadata.service.ServicePartitionMetadata;
import com.slack.kaldb.proto.manager_api.ManagerApi;
import com.slack.kaldb.proto.manager_api.ManagerApiServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Administration API for managing service configurations, including throughput and partition
 * assignments. This API is available only on the cluster manager service, and the data created is
 * consumed primarily by the pre-processor and query services.
 */
public class ManagerApiGrpc extends ManagerApiServiceGrpc.ManagerApiServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(ManagerApiGrpc.class);

  private final ServiceMetadataStore serviceMetadataStore;

  public ManagerApiGrpc(ServiceMetadataStore serviceMetadataStore) {
    this.serviceMetadataStore = serviceMetadataStore;
  }

  @Override
  public void createService(
      ManagerApi.CreateServiceRequest request,
      StreamObserver<ManagerApi.CreateServiceResponse> responseObserver) {
    ServiceRequestContext.current()
        .blockingTaskExecutor()
        .submit(
            () -> {
              try {
                serviceMetadataStore.createSync(
                    new ServiceMetadata(
                        request.getName(),
                        request.getOwner(),
                        request.getThroughputBytes(),
                        Collections.emptyList()));
                responseObserver.onNext(ManagerApi.CreateServiceResponse.newBuilder().build());
                responseObserver.onCompleted();
              } catch (Exception e) {
                LOG.error("Error creating new service", e);
                responseObserver.onError(
                    Status.UNKNOWN.withDescription(e.getMessage()).asException());
              }
            });
  }

  @Override
  public void getService(
      ManagerApi.GetServiceRequest request,
      StreamObserver<ManagerApi.GetServiceResponse> responseObserver) {
    ServiceRequestContext.current()
        .blockingTaskExecutor()
        .submit(
            () -> {
              try {
                ServiceMetadata serviceMetadata =
                    serviceMetadataStore.getNodeSync(request.getName());
                responseObserver.onNext(
                    ManagerApi.GetServiceResponse.newBuilder()
                        .setName(serviceMetadata.getName())
                        .setOwner(serviceMetadata.getOwner())
                        .setThroughputBytes(serviceMetadata.getThroughputBytes())
                        .build());
                responseObserver.onCompleted();
              } catch (Exception e) {
                LOG.error("Error getting service", e);
                responseObserver.onError(
                    Status.UNKNOWN.withDescription(e.getMessage()).asException());
              }
            });
  }

  @Override
  public void addServicePartition(
      ManagerApi.AddServicePartitionRequest request,
      StreamObserver<ManagerApi.AddServicePartitionResponse> responseObserver) {
    ServiceRequestContext.current()
        .blockingTaskExecutor()
        .submit(
            () -> {
              try {
                ServiceMetadata serviceMetadata =
                    serviceMetadataStore.getNodeSync(request.getName());
                ServicePartitionMetadata servicePartitionMetadata =
                    new ServicePartitionMetadata(
                        // todo - should this have a fixed amount of padding applied?
                        Instant.now().toEpochMilli(),
                        Long.MAX_VALUE,
                        request.getPartitionIdsList());
                ImmutableList<ServicePartitionMetadata> updatedServicePartitionMetadata =
                    ImmutableList.<ServicePartitionMetadata>builder()
                        .addAll(serviceMetadata.getPartitionConfigs())
                        .add(servicePartitionMetadata)
                        .build();

                ServiceMetadata updatedServiceMetadata =
                    new ServiceMetadata(
                        serviceMetadata.getName(),
                        serviceMetadata.getOwner(),
                        serviceMetadata.getThroughputBytes(),
                        updatedServicePartitionMetadata);

                serviceMetadataStore.updateSync(updatedServiceMetadata);
                responseObserver.onNext(
                    ManagerApi.AddServicePartitionResponse.newBuilder().build());
                responseObserver.onCompleted();
              } catch (Exception e) {
                LOG.error("Error adding service partition", e);
                responseObserver.onError(
                    Status.UNKNOWN.withDescription(e.getMessage()).asException());
              }
            });
  }

  @Override
  public void getServicePartitions(
      ManagerApi.GetServicePartitionsRequest request,
      StreamObserver<ManagerApi.GetServicePartitionsResponse> responseObserver) {
    ServiceRequestContext.current()
        .blockingTaskExecutor()
        .submit(
            () -> {
              try {
                List<ManagerApi.GetServicePartitionsResponse.ServicePartition> partitions =
                    serviceMetadataStore
                        .getNodeSync(request.getName())
                        .getPartitionConfigs()
                        .stream()
                        .map(
                            servicePartitionMetadata ->
                                ManagerApi.GetServicePartitionsResponse.ServicePartition
                                    .newBuilder()
                                    .setStartTimeEpochMs(
                                        servicePartitionMetadata.getStartTimeEpochMs())
                                    .setEndTimeEpochMs(servicePartitionMetadata.getEndTimeEpochMs())
                                    .addAllPartitionIds(servicePartitionMetadata.getPartitions())
                                    .build())
                        .collect(Collectors.toList());

                responseObserver.onNext(
                    ManagerApi.GetServicePartitionsResponse.newBuilder()
                        .addAllServicePartitions(partitions)
                        .build());
                responseObserver.onCompleted();
              } catch (Exception e) {
                LOG.error("Error getting service partitions", e);
                responseObserver.onError(
                    Status.UNKNOWN.withDescription(e.getMessage()).asException());
              }
            });
  }

  @Override
  public void getServices(
      ManagerApi.GetServicesRequest request,
      StreamObserver<ManagerApi.GetServicesResponse> responseObserver) {
    ServiceRequestContext.current()
        .blockingTaskExecutor()
        .submit(
            () -> {
              try {
                List<ManagerApi.GetServicesResponse.ServiceResponse> serviceResponseList =
                    serviceMetadataStore
                        .listSync()
                        .stream()
                        .map(
                            serviceMetadata ->
                                ManagerApi.GetServicesResponse.ServiceResponse.newBuilder()
                                    .setName(serviceMetadata.getName())
                                    .setOwner(serviceMetadata.getOwner())
                                    .setThroughputBytes(serviceMetadata.getThroughputBytes())
                                    .build())
                        .collect(Collectors.toList());

                responseObserver.onNext(
                    ManagerApi.GetServicesResponse.newBuilder()
                        .addAllServiceList(serviceResponseList)
                        .build());
                responseObserver.onCompleted();
              } catch (Exception e) {
                LOG.error("Error getting services", e);
                responseObserver.onError(
                    Status.UNKNOWN.withDescription(e.getMessage()).asException());
              }
            });
  }
}
