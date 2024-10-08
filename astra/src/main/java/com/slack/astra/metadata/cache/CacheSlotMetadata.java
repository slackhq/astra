package com.slack.astra.metadata.cache;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.astra.metadata.core.AstraPartitionedMetadata;
import com.slack.astra.proto.metadata.Metadata;
import java.util.Objects;

/**
 * TODO: Currently, application code directly manipulates cache slot states which is error prone.
 * Make transitions more controlled via a state machine like API.
 */
public class CacheSlotMetadata extends AstraPartitionedMetadata {
  public final String hostname;
  public final String replicaSet;
  public final Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState;
  public final String replicaId;
  public final long updatedTimeEpochMs;

  public CacheSlotMetadata(
      String name,
      Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState,
      String replicaId,
      long updatedTimeEpochMs,
      String hostname,
      String replicaSet) {
    super(name);
    checkArgument(hostname != null && !hostname.isEmpty(), "Hostname cannot be null or empty");
    checkArgument(cacheSlotState != null, "Cache slot state cannot be null");
    checkArgument(updatedTimeEpochMs > 0, "Updated time must be greater than 0");
    if (cacheSlotState.equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE)) {
      checkArgument(
          replicaId != null && replicaId.isEmpty(),
          "If cache slot is free replicaId must be empty");
    } else {
      checkArgument(
          replicaId != null && !replicaId.isEmpty(),
          "If cache slot is not free, replicaId must not be empty");
    }

    this.hostname = hostname;
    this.replicaSet = replicaSet;
    this.cacheSlotState = cacheSlotState;
    this.replicaId = replicaId;
    this.updatedTimeEpochMs = updatedTimeEpochMs;
  }

  public String getHostname() {
    return hostname;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CacheSlotMetadata that)) return false;
    if (!super.equals(o)) return false;

    if (updatedTimeEpochMs != that.updatedTimeEpochMs) return false;
    if (!hostname.equals(that.hostname)) return false;
    if (!Objects.equals(replicaSet, that.replicaSet)) return false;
    if (cacheSlotState != that.cacheSlotState) return false;
    return replicaId.equals(that.replicaId);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + hostname.hashCode();
    result = 31 * result + (replicaSet != null ? replicaSet.hashCode() : 0);
    result = 31 * result + cacheSlotState.hashCode();
    result = 31 * result + replicaId.hashCode();
    result = 31 * result + (int) (updatedTimeEpochMs ^ (updatedTimeEpochMs >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "CacheSlotMetadata{"
        + "hostname='"
        + hostname
        + '\''
        + ", replicaSet='"
        + replicaSet
        + '\''
        + ", cacheSlotState="
        + cacheSlotState
        + ", replicaId='"
        + replicaId
        + '\''
        + ", updatedTimeEpochMs="
        + updatedTimeEpochMs
        + ", name='"
        + name
        + '\''
        + '}';
  }

  @Override
  public String getPartition() {
    return hostname;
  }
}
