package com.slack.kaldb.metadata.service;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.slack.kaldb.metadata.core.KaldbMetadata;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Configurations for an upstream service for use in the pre-processor - including rate limits, and
 * partition mapping.
 */
public class ServiceMetadata extends KaldbMetadata {

  public final ImmutableList<ServicePartitionMetadata> partitionList;

  public ServiceMetadata(String name, List<ServicePartitionMetadata> partitionList) {
    super(name);
    checkArgument(partitionList != null, "partitionList must not be null");
    checkPartitions(name, partitionList);

    this.partitionList = ImmutableList.copyOf(partitionList);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ServiceMetadata that = (ServiceMetadata) o;
    return partitionList.equals(that.partitionList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), partitionList);
  }

  @Override
  public String toString() {
    return "ServiceMetadata{" + "name='" + name + '\'' + ", partitionList=" + partitionList + '}';
  }

  /** Ensures the provided partition list contains a valid configuration */
  private void checkPartitions(String name, List<ServicePartitionMetadata> partitionList) {
    Map<String, List<ServicePartitionMetadata>> groupedPartitionsByServiceName =
        partitionList.stream().collect(Collectors.groupingBy(ServicePartitionMetadata::getName));

    for (Map.Entry<String, List<ServicePartitionMetadata>> entry :
        groupedPartitionsByServiceName.entrySet()) {
      List<ServicePartitionMetadata> sortedPartitionsByStartTime =
          entry
              .getValue()
              .stream()
              .sorted(Comparator.comparingLong(ServicePartitionMetadata::getStartTimeEpochMs))
              .collect(Collectors.toList());

      for (int i = 0; i < sortedPartitionsByStartTime.size(); i++) {
        if (i + 1 != sortedPartitionsByStartTime.size()) {
          checkArgument(
              sortedPartitionsByStartTime.get(i).endTimeEpochMs
                  < sortedPartitionsByStartTime.get(i + 1).startTimeEpochMs,
              String.format(
                  "Service '%s' has an invalid partition configuration for partition '%s' - there is an overlap in partition times [%s]",
                  name, entry.getKey(), entry.getValue().toString()));
        }
      }
    }
  }
}
