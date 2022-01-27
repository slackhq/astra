package com.slack.kaldb.metadata.cache;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.proto.metadata.Metadata;

public class CacheSlotMetadata extends KaldbMetadata {
  public final Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState;
  public final String replicaId;
  public final long updatedTimeEpochMs;

  public CacheSlotMetadata(
      String name,
      Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState,
      String replicaId,
      long updatedTimeEpochMs) {
    super(name);
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

    this.cacheSlotState = cacheSlotState;
    this.replicaId = replicaId;
    this.updatedTimeEpochMs = updatedTimeEpochMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    CacheSlotMetadata that = (CacheSlotMetadata) o;

    if (!cacheSlotState.equals(that.cacheSlotState)) return false;
    if (updatedTimeEpochMs != that.updatedTimeEpochMs) return false;
    return replicaId.equals(that.replicaId);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + cacheSlotState.hashCode();
    result = 31 * result + replicaId.hashCode();
    result = 31 * result + (int) (updatedTimeEpochMs ^ (updatedTimeEpochMs >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "CacheSlotMetadata{"
        + "cacheSlotState="
        + cacheSlotState
        + ", replicaId='"
        + replicaId
        + '\''
        + ", updatedTimeEpochMs='"
        + updatedTimeEpochMs
        + ", name='"
        + name
        + '\''
        + '}';
  }
}
