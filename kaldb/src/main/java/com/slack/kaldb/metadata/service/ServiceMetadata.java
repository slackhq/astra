package com.slack.kaldb.metadata.service;

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
public class ServiceMetadata extends KaldbMetadata {

  public final String owner;
  public final long throughputBytes;
  public final ImmutableList<ServicePartitionMetadata> partitionConfigs;

  public ServiceMetadata(
      String name,
      String owner,
      long throughputBytes,
      List<ServicePartitionMetadata> partitionConfigs) {
    super(name);
    checkArgument(partitionConfigs != null, "partitionConfigs must not be null");
    checkArgument(owner != null && !owner.isBlank(), "owner must not be null or blank");
    checkArgument(throughputBytes > 0, "throughputBytes must be greater than 0");
    checkPartitions(partitionConfigs, "partitionConfigs must not overlap start and end times");

    this.owner = owner;
    this.throughputBytes = throughputBytes;
    this.partitionConfigs = ImmutableList.copyOf(partitionConfigs);
  }

  public String getOwner() {
    return owner;
  }

  public long getThroughputBytes() {
    return throughputBytes;
  }

  public ImmutableList<ServicePartitionMetadata> getPartitionConfigs() {
    return partitionConfigs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ServiceMetadata that = (ServiceMetadata) o;
    return throughputBytes == that.throughputBytes
        && owner.equals(that.owner)
        && partitionConfigs.equals(that.partitionConfigs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), owner, throughputBytes, partitionConfigs);
  }

  @Override
  public String toString() {
    return "ServiceMetadata{"
        + "name='"
        + name
        + '\''
        + ", owner='"
        + owner
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
      List<ServicePartitionMetadata> partitionConfig, String errorMessage) {
    List<ServicePartitionMetadata> sortedConfigsByStartTime =
        partitionConfig
            .stream()
            .sorted(Comparator.comparingLong(ServicePartitionMetadata::getStartTimeEpochMs))
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
