package com.slack.kaldb.metadata.service;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.slack.kaldb.proto.metadata.Metadata;
import java.util.List;
import java.util.Objects;

/**
 * Metadata for a specific partition configuration at a point in time. For partitions that are
 * currently active we would expect to have an endTime of max long.
 */
public class DatasetPartitionMetadata {

  public final long startTimeEpochMs;
  public final long endTimeEpochMs;
  public final ImmutableList<String> partitions;

  public DatasetPartitionMetadata(
      long startTimeEpochMs, long endTimeEpochMs, List<String> partitions) {
    checkArgument(startTimeEpochMs > 0, "startTimeEpochMs must be greater than 0");
    checkArgument(
        endTimeEpochMs > startTimeEpochMs,
        "endTimeEpochMs must be greater than the startTimeEpochMs");
    checkArgument(partitions != null, "partitions must be non-null");

    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.partitions = ImmutableList.copyOf(partitions);
  }

  public long getStartTimeEpochMs() {
    return startTimeEpochMs;
  }

  public long getEndTimeEpochMs() {
    return endTimeEpochMs;
  }

  public ImmutableList<String> getPartitions() {
    return partitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DatasetPartitionMetadata that = (DatasetPartitionMetadata) o;
    return startTimeEpochMs == that.startTimeEpochMs
        && endTimeEpochMs == that.endTimeEpochMs
        && partitions.equals(that.partitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTimeEpochMs, endTimeEpochMs, partitions);
  }

  @Override
  public String toString() {
    return "ServicePartitionMetadata{"
        + "startTimeEpochMs="
        + startTimeEpochMs
        + ", endTimeEpochMs="
        + endTimeEpochMs
        + ", partitions="
        + partitions
        + '}';
  }

  public static DatasetPartitionMetadata fromServicePartitionMetadataProto(
      Metadata.DatasetPartitionMetadata servicePartitionMetadata) {
    return new DatasetPartitionMetadata(
        servicePartitionMetadata.getStartTimeEpochMs(),
        servicePartitionMetadata.getEndTimeEpochMs(),
        servicePartitionMetadata.getPartitionsList());
  }

  public static Metadata.DatasetPartitionMetadata toServicePartitionMetadataProto(
      DatasetPartitionMetadata metadata) {
    return Metadata.DatasetPartitionMetadata.newBuilder()
        .setStartTimeEpochMs(metadata.startTimeEpochMs)
        .setEndTimeEpochMs(metadata.endTimeEpochMs)
        .addAllPartitions(metadata.partitions)
        .build();
  }
}
