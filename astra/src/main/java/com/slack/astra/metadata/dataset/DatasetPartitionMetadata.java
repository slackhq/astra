package com.slack.astra.metadata.dataset;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.slack.astra.chunk.ChunkInfo;
import com.slack.astra.proto.metadata.Metadata;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Metadata for a specific partition configuration at a point in time. For partitions that are
 * currently active we would expect to have an endTime of max long.
 */
public class DatasetPartitionMetadata {

  public final long startTimeEpochMs;
  public final long endTimeEpochMs;
  public final ImmutableList<String> partitions;

  public static final String MATCH_ALL_DATASET = "_all";

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
    return "DatasetPartitionMetadata{"
        + "startTimeEpochMs="
        + startTimeEpochMs
        + ", endTimeEpochMs="
        + endTimeEpochMs
        + ", partitions="
        + partitions
        + '}';
  }

  public static DatasetPartitionMetadata fromDatasetPartitionMetadataProto(
      Metadata.DatasetPartitionMetadata datasetPartitionMetadata) {
    return new DatasetPartitionMetadata(
        datasetPartitionMetadata.getStartTimeEpochMs(),
        datasetPartitionMetadata.getEndTimeEpochMs(),
        datasetPartitionMetadata.getPartitionsList());
  }

  public static Metadata.DatasetPartitionMetadata toDatasetPartitionMetadataProto(
      DatasetPartitionMetadata metadata) {
    return Metadata.DatasetPartitionMetadata.newBuilder()
        .setStartTimeEpochMs(metadata.startTimeEpochMs)
        .setEndTimeEpochMs(metadata.endTimeEpochMs)
        .addAllPartitions(metadata.partitions)
        .build();
  }

  /**
   * Get partitions that match on two criteria 1. index name 2. partitions that have an overlap with
   * the query window.
   */
  public static List<DatasetPartitionMetadata> findPartitionsToQuery(
      DatasetMetadataStore datasetMetadataStore,
      long startTimeEpochMs,
      long endTimeEpochMs,
      String dataset) {
    boolean skipDatasetFilter = (dataset.equals("*") || dataset.equals(MATCH_ALL_DATASET));
    return datasetMetadataStore.listSync().stream()
        .filter(serviceMetadata -> skipDatasetFilter || serviceMetadata.name.equals(dataset))
        .flatMap(
            serviceMetadata -> serviceMetadata.partitionConfigs.stream()) // will always return one
        .filter(
            partitionMetadata ->
                ChunkInfo.containsDataInTimeRange(
                    partitionMetadata.startTimeEpochMs,
                    partitionMetadata.endTimeEpochMs,
                    startTimeEpochMs,
                    endTimeEpochMs))
        .collect(Collectors.toList());
  }
}
