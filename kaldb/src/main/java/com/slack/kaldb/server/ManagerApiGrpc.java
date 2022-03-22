package com.slack.kaldb.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
import java.util.Optional;
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
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void updateService(
      ManagerApi.UpdateServiceRequest request,
      StreamObserver<ManagerApi.UpdateServiceResponse> responseObserver) {

    try {
      ServiceMetadata existingServiceMetadata = serviceMetadataStore.getNodeSync(request.getName());

      ServiceMetadata updatedServiceMetadata =
          new ServiceMetadata(
              existingServiceMetadata.getName(),
              request.getOwner(),
              request.getThroughputBytes(),
              existingServiceMetadata.getPartitionConfigs());
      serviceMetadataStore.updateSync(updatedServiceMetadata);

      responseObserver.onNext(
          ManagerApi.UpdateServiceResponse.newBuilder()
              .setName(updatedServiceMetadata.getName())
              .setOwner(updatedServiceMetadata.getOwner())
              .setThroughputBytes(updatedServiceMetadata.getThroughputBytes())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error updating existing service", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void getService(
      ManagerApi.GetServiceRequest request,
      StreamObserver<ManagerApi.GetServiceResponse> responseObserver) {

    try {
      ServiceMetadata serviceMetadata = serviceMetadataStore.getNodeSync(request.getName());
      responseObserver.onNext(
          ManagerApi.GetServiceResponse.newBuilder()
              .setName(serviceMetadata.getName())
              .setOwner(serviceMetadata.getOwner())
              .setThroughputBytes(serviceMetadata.getThroughputBytes())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error getting service", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void addServicePartition(
      ManagerApi.AddServicePartitionRequest request,
      StreamObserver<ManagerApi.AddServicePartitionResponse> responseObserver) {
    try {
      Preconditions.checkArgument(
          request.getPartitionIdsList().size() > 0, "PartitionIds list must not be empty");
      Preconditions.checkArgument(
          request.getPartitionIdsList().stream().noneMatch(String::isBlank),
          "PartitionIds list must not contain blank strings");

      ServiceMetadata serviceMetadata = serviceMetadataStore.getNodeSync(request.getName());
      Optional<ServicePartitionMetadata> previousActiveServicePartition =
          serviceMetadata
              .getPartitionConfigs()
              .stream()
              .filter(
                  servicePartitionMetadata ->
                      servicePartitionMetadata.getEndTimeEpochMs() == Long.MAX_VALUE)
              .findFirst();
      List<ServicePartitionMetadata> remainingServicePartitions =
          serviceMetadata
              .getPartitionConfigs()
              .stream()
              .filter(
                  servicePartitionMetadata ->
                      servicePartitionMetadata.getEndTimeEpochMs() != Long.MAX_VALUE)
              .collect(Collectors.toList());

      // todo - consider adding some padding to this value; this may complicate
      //   validation as you would need to consider what happens when there's a future
      //   cut-over already scheduled
      long partitionCutoverTime = Instant.now().toEpochMilli();

      ImmutableList.Builder<ServicePartitionMetadata> builder =
          ImmutableList.<ServicePartitionMetadata>builder().addAll(remainingServicePartitions);
      if (previousActiveServicePartition.isPresent()) {
        ServicePartitionMetadata updatedPreviousActivePartition =
            new ServicePartitionMetadata(
                previousActiveServicePartition.get().getStartTimeEpochMs(),
                partitionCutoverTime,
                previousActiveServicePartition.get().getPartitions());
        builder.add(updatedPreviousActivePartition);
      }

      ServicePartitionMetadata newPartitionMetadata =
          new ServicePartitionMetadata(
              partitionCutoverTime + 1, Long.MAX_VALUE, request.getPartitionIdsList());
      ImmutableList<ServicePartitionMetadata> updatedServicePartitionMetadata =
          builder.add(newPartitionMetadata).build();

      ServiceMetadata updatedServiceMetadata =
          new ServiceMetadata(
              serviceMetadata.getName(),
              serviceMetadata.getOwner(),
              serviceMetadata.getThroughputBytes(),
              updatedServicePartitionMetadata);

      serviceMetadataStore.updateSync(updatedServiceMetadata);
      responseObserver.onNext(
          ManagerApi.AddServicePartitionResponse.newBuilder()
              .setStartTimeEpochMs(newPartitionMetadata.getStartTimeEpochMs())
              .addAllPartitionIds(newPartitionMetadata.getPartitions())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error adding service partition", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void getServicePartitions(
      ManagerApi.GetServicePartitionsRequest request,
      StreamObserver<ManagerApi.GetServicePartitionsResponse> responseObserver) {
    try {
      List<ManagerApi.GetServicePartitionsResponse.ServicePartition> partitions =
          serviceMetadataStore
              .getNodeSync(request.getName())
              .getPartitionConfigs()
              .stream()
              .map(
                  servicePartitionMetadata ->
                      ManagerApi.GetServicePartitionsResponse.ServicePartition.newBuilder()
                          .setStartTimeEpochMs(servicePartitionMetadata.getStartTimeEpochMs())
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
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void getServices(
      ManagerApi.GetServicesRequest request,
      StreamObserver<ManagerApi.GetServicesResponse> responseObserver) {

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
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }
}
