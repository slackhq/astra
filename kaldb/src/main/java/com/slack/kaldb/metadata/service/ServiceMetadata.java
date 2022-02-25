package com.slack.kaldb.metadata.service;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import java.util.List;
import java.util.Objects;

public class ServiceMetadata extends KaldbMetadata {

  public final List<ServicePartitionMetadata> partitionList;

  public ServiceMetadata(String name, List<ServicePartitionMetadata> partitionList) {
    super(name);
    checkArgument(partitionList != null, "partitionList must not be null");
    this.partitionList = partitionList;
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
}
