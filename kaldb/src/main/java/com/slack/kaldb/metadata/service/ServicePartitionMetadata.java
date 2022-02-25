package com.slack.kaldb.metadata.service;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.proto.metadata.Metadata;
import java.util.Objects;

/**
 * Metadata for a specific partition configuration at a point in time. For partitions that are
 * currently active we would expect to have an endTime of max long.
 */
public class ServicePartitionMetadata {

  public final String name;
  public final long throughputBytes;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;

  public ServicePartitionMetadata(
      String name, long throughputBytes, long startTimeEpochMs, long endTimeEpochMs) {
    checkArgument(name != null && !name.isEmpty(), "name can't be null or empty.");
    checkArgument(throughputBytes > 0, "throughputBytes must be greater than 0");
    checkArgument(startTimeEpochMs > 0, "startTimeEpochMs must be greater than 0");
    checkArgument(
        endTimeEpochMs > startTimeEpochMs,
        "endTimeEpochMs must be greater than the startTimeEpochMs");

    this.name = name;
    this.throughputBytes = throughputBytes;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
  }

  public String getName() {
    return name;
  }

  public long getStartTimeEpochMs() {
    return startTimeEpochMs;
  }

  public long getEndTimeEpochMs() {
    return endTimeEpochMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServicePartitionMetadata that = (ServicePartitionMetadata) o;
    return throughputBytes == that.throughputBytes
        && startTimeEpochMs == that.startTimeEpochMs
        && endTimeEpochMs == that.endTimeEpochMs
        && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, throughputBytes, startTimeEpochMs, endTimeEpochMs);
  }

  @Override
  public String toString() {
    return "ServicePartitionMetadata{"
        + "name='"
        + name
        + '\''
        + ", throughputBytes="
        + throughputBytes
        + ", startTimeEpochMs="
        + startTimeEpochMs
        + ", endTimeEpochMs="
        + endTimeEpochMs
        + '}';
  }

  public static ServicePartitionMetadata fromServicePartitionMetadataProto(
      Metadata.ServicePartitionMetadata servicePartitionMetadata) {
    return new ServicePartitionMetadata(
        servicePartitionMetadata.getName(),
        servicePartitionMetadata.getThroughputBytes(),
        servicePartitionMetadata.getStartTimeEpochMs(),
        servicePartitionMetadata.getEndTimeEpochMs());
  }

  public static Metadata.ServicePartitionMetadata toServicePartitionMetadataProto(
      ServicePartitionMetadata metadata) {
    return Metadata.ServicePartitionMetadata.newBuilder()
        .setName(metadata.name)
        .setThroughputBytes(metadata.throughputBytes)
        .setStartTimeEpochMs(metadata.startTimeEpochMs)
        .setEndTimeEpochMs(metadata.endTimeEpochMs)
        .build();
  }
}
