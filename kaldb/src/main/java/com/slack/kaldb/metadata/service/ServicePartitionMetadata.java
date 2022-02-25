package com.slack.kaldb.metadata.service;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import java.util.Objects;

public class ServicePartitionMetadata extends KaldbMetadata {

  public final long throughputBytes;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;

  public ServicePartitionMetadata(
      String name, long throughputBytes, long startTimeEpochMs, long endTimeEpochMs) {
    super(name);

    checkArgument(throughputBytes > 0, "throughputBytes must be greater than 0");
    checkArgument(startTimeEpochMs > 0, "startTimeEpochMs must be greater than 0");
    checkArgument(
        endTimeEpochMs > startTimeEpochMs,
        "endTimeEpochMs must be greater than the startTimeEpochMs");

    this.throughputBytes = throughputBytes;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ServicePartitionMetadata that = (ServicePartitionMetadata) o;
    return throughputBytes == that.throughputBytes
        && startTimeEpochMs == that.startTimeEpochMs
        && endTimeEpochMs == that.endTimeEpochMs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), throughputBytes, startTimeEpochMs, endTimeEpochMs);
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
}
