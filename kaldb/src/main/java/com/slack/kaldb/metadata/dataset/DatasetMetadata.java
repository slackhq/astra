package com.slack.kaldb.metadata.dataset;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.slack.kaldb.metadata.core.KaldbMetadata;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Contains configurations for use in the pre-processor and query service - including rate limits,
 * and partition mapping.
 */
public class DatasetMetadata extends KaldbMetadata {

  public final String owner;
  public final String serviceName;
  public final long throughputBytes;
  public final ImmutableList<DatasetPartitionMetadata> partitionConfigs;

  public DatasetMetadata(
      String name,
      String owner,
      long throughputBytes,
      List<DatasetPartitionMetadata> partitionConfigs,
      String serviceName) {
    super(name);
    checkArgument(name.length() <= 256, "name must be no longer than 256 chars");
    checkArgument(name.matches("^[a-zA-Z0-9_-]*$"), "name must contain only [a-zA-Z0-9_-]");
    checkArgument(partitionConfigs != null, "partitionConfigs must not be null");
    checkArgument(owner != null && !owner.isBlank(), "owner must not be null or blank");
    checkArgument(throughputBytes >= 0, "throughputBytes must be greater than or equal to 0");
    checkPartitions(partitionConfigs, "partitionConfigs must not overlap start and end times");

    checkArgument(serviceName.length() <= 256, "serviceName must be no longer than 256 chars");
    checkArgument(
        serviceName.matches("^[a-zA-Z0-9_-]*$"), "serviceName must contain only [a-zA-Z0-9_-]");

    this.owner = owner;
    this.serviceName = serviceName;
    this.throughputBytes = throughputBytes;
    this.partitionConfigs = ImmutableList.copyOf(partitionConfigs);
  }

  public DatasetMetadata getDataset() {
    return this;
  }

  public String getOwner() {
    return owner;
  }

  public long getThroughputBytes() {
    return throughputBytes;
  }

  public ImmutableList<DatasetPartitionMetadata> getPartitionConfigs() {
    return partitionConfigs;
  }

  public String getServiceName() {
    return serviceName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DatasetMetadata)) return false;
    if (!super.equals(o)) return false;
    DatasetMetadata that = (DatasetMetadata) o;
    return throughputBytes == that.throughputBytes
        && name.equals(that.name)
        && owner.equals(that.owner)
        && serviceName.equals(that.serviceName)
        && partitionConfigs.equals(that.partitionConfigs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), name, owner, serviceName, throughputBytes, partitionConfigs);
  }

  @Override
  public String toString() {
    return "DatasetMetadata{"
        + "name='"
        + name
        + '\''
        + ", owner='"
        + owner
        + '\''
        + ", serviceName='"
        + serviceName
        + '\''
        + ", throughputBytes="
        + throughputBytes
        + ", partitionConfigs="
        + partitionConfigs
        + '}';
  }

  /**
   * Check that the list of partitionConfigs do not overlap start and end times. This sorts the list
   * by start of partitions by start time, and then ensures that the end of a given item does not
   * overlap with the start of the next item in the list.
   */
  private void checkPartitions(
      List<DatasetPartitionMetadata> partitionConfig, String errorMessage) {
    List<DatasetPartitionMetadata> sortedConfigsByStartTime =
        partitionConfig
            .stream()
            .sorted(Comparator.comparingLong(DatasetPartitionMetadata::getStartTimeEpochMs))
            .collect(Collectors.toList());

    for (int i = 0; i < sortedConfigsByStartTime.size(); i++) {
      if (i + 1 != sortedConfigsByStartTime.size()) {
        checkArgument(
            sortedConfigsByStartTime.get(i).endTimeEpochMs
                < sortedConfigsByStartTime.get(i + 1).startTimeEpochMs,
            errorMessage);
      }
    }
  }
}
