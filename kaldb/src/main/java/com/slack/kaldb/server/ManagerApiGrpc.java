package com.slack.kaldb.server;

import static com.slack.kaldb.metadata.service.ServiceMetadataSerializer.toServiceMetadataProto;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.slack.kaldb.metadata.service.ServiceMetadata;
import com.slack.kaldb.metadata.service.ServiceMetadataSerializer;
import com.slack.kaldb.metadata.service.ServiceMetadataStore;
import com.slack.kaldb.metadata.service.ServicePartitionMetadata;
import com.slack.kaldb.proto.manager_api.ManagerApi;
import com.slack.kaldb.proto.manager_api.ManagerApiServiceGrpc;
import com.slack.kaldb.proto.metadata.Metadata;
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
  public static final long MAX_TIME = Long.MAX_VALUE;

  public ManagerApiGrpc(ServiceMetadataStore serviceMetadataStore) {
    this.serviceMetadataStore = serviceMetadataStore;
  }

  /** Initializes a new service in the metadata store with no initial allocated capacity */
  @Override
  public void createServiceMetadata(
      ManagerApi.CreateServiceMetadataRequest request,
      StreamObserver<Metadata.ServiceMetadata> responseObserver) {

    try {
      serviceMetadataStore.createSync(
          new ServiceMetadata(request.getName(), request.getOwner(), 0L, Collections.emptyList()));
      responseObserver.onNext(
          toServiceMetadataProto(serviceMetadataStore.getNodeSync(request.getName())));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error creating new service", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Updates an existing service with new metadata */
  @Override
  public void updateServiceMetadata(
      ManagerApi.UpdateServiceMetadataRequest request,
      StreamObserver<Metadata.ServiceMetadata> responseObserver) {

    try {
      ServiceMetadata existingServiceMetadata = serviceMetadataStore.getNodeSync(request.getName());

      ServiceMetadata updatedServiceMetadata =
          new ServiceMetadata(
              existingServiceMetadata.getName(),
              request.getOwner(),
              existingServiceMetadata.getThroughputBytes(),
              existingServiceMetadata.getPartitionConfigs());
      serviceMetadataStore.updateSync(updatedServiceMetadata);
      responseObserver.onNext(toServiceMetadataProto(updatedServiceMetadata));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error updating existing service", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Returns a single service metadata by name */
  @Override
  public void getServiceMetadata(
      ManagerApi.GetServiceMetadataRequest request,
      StreamObserver<Metadata.ServiceMetadata> responseObserver) {

    try {
      responseObserver.onNext(
          toServiceMetadataProto(serviceMetadataStore.getNodeSync(request.getName())));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error getting service", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Returns all available services from the metadata store */
  @Override
  public void listServiceMetadata(
      ManagerApi.ListServiceMetadataRequest request,
      StreamObserver<ManagerApi.ListServiceMetadataResponse> responseObserver) {
    // todo - consider adding search/pagination support
    try {
      responseObserver.onNext(
          ManagerApi.ListServiceMetadataResponse.newBuilder()
              .addAllServiceMetadata(
                  serviceMetadataStore
                      .listSync()
                      .stream()
                      .map(ServiceMetadataSerializer::toServiceMetadataProto)
                      .collect(Collectors.toList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error getting services", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /**
   * Allocates a new partition assignment for a service. If a rate and a list of partition IDs are
   * provided, it will use it use the list of partition ids as the current allocation and
   * invalidates the existing assignment.
   */
  @Override
  public void updatePartitionAssignment(
      ManagerApi.UpdatePartitionAssignmentRequest request,
      StreamObserver<ManagerApi.UpdatePartitionAssignmentResponse> responseObserver) {
    // todo - In the future if only a rate is provided with an empty list the allocation
    //  will be automatically assigned.

    try {
      // todo - add additional validation to ensure the provided allocation makes sense for the
      //  configured throughput values. If no partitions are provided, auto-allocate.
      Preconditions.checkArgument(
          request.getPartitionIdsList().stream().noneMatch(String::isBlank),
          "PartitionIds list must not contain blank strings");

      ServiceMetadata serviceMetadata = serviceMetadataStore.getNodeSync(request.getName());
      ImmutableList<ServicePartitionMetadata> updatedServicePartitionMetadata =
          addNewPartition(serviceMetadata.getPartitionConfigs(), request.getPartitionIdsList());

      // if the user provided a non-negative value for throughput set it, otherwise default to the
      // existing value
      long updatedThroughputBytes =
          request.getThroughputBytes() < 0
              ? serviceMetadata.getThroughputBytes()
              : request.getThroughputBytes();

      ServiceMetadata updatedServiceMetadata =
          new ServiceMetadata(
              serviceMetadata.getName(),
              serviceMetadata.getOwner(),
              updatedThroughputBytes,
              updatedServicePartitionMetadata);
      serviceMetadataStore.updateSync(updatedServiceMetadata);

      responseObserver.onNext(
          ManagerApi.UpdatePartitionAssignmentResponse.newBuilder()
              .addAllAssignedPartitionIds(request.getPartitionIdsList())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error updating partition assignment", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /**
   * Returns a new list of service partition metadata, with the provided partition IDs as the
   * current active assignment. This finds the current active assignment (end time of max long),
   * sets it to the current time, and then appends a new service partition assignment starting from
   * current time + 1 to max long.
   */
  private static ImmutableList<ServicePartitionMetadata> addNewPartition(
      List<ServicePartitionMetadata> existingPartitions, List<String> newPartitionIdsList) {
    if (newPartitionIdsList.isEmpty()) {
      return ImmutableList.copyOf(existingPartitions);
    }

    Optional<ServicePartitionMetadata> previousActiveServicePartition =
        existingPartitions
            .stream()
            .filter(
                servicePartitionMetadata ->
                    servicePartitionMetadata.getEndTimeEpochMs() == MAX_TIME)
            .findFirst();

    List<ServicePartitionMetadata> remainingServicePartitions =
        existingPartitions
            .stream()
            .filter(
                servicePartitionMetadata ->
                    servicePartitionMetadata.getEndTimeEpochMs() != MAX_TIME)
            .collect(Collectors.toList());

    // todo - consider adding some padding to this value; this may complicate
    //   validation as you would need to consider what happens when there's a future
    //   cut-over already scheduled
    // todo - if introducing an optional padding this should be added as a method parameter
    //   see https://github.com/slackhq/kaldb/pull/244#discussion_r835424863
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
        new ServicePartitionMetadata(partitionCutoverTime + 1, MAX_TIME, newPartitionIdsList);
    return builder.add(newPartitionMetadata).build();
  }
}
