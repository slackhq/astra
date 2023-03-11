package com.slack.kaldb.metadata.cache;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Objects;
import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.proto.metadata.Metadata;
import java.util.List;

public class CacheSlotMetadata extends KaldbMetadata {
  public final Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState;
  public final String replicaId;
  public final long updatedTimeEpochMs;
  public final List<Metadata.IndexType> supportedIndexTypes;

  public CacheSlotMetadata(
      String name,
      Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState,
      String replicaId,
      long updatedTimeEpochMs,
      List<Metadata.IndexType> supportedIndexTypes) {
    super(name);
    checkArgument(cacheSlotState != null, "Cache slot state cannot be null");
    checkArgument(updatedTimeEpochMs > 0, "Updated time must be greater than 0");
    checkArgument(
        supportedIndexTypes != null && !supportedIndexTypes.isEmpty(),
        "supported index types shouldn't be empty");
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
    this.supportedIndexTypes = supportedIndexTypes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    CacheSlotMetadata that = (CacheSlotMetadata) o;
    return updatedTimeEpochMs == that.updatedTimeEpochMs
        && cacheSlotState == that.cacheSlotState
        && Objects.equal(replicaId, that.replicaId)
        && Objects.equal(supportedIndexTypes, that.supportedIndexTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(), cacheSlotState, replicaId, updatedTimeEpochMs, supportedIndexTypes);
  }

  @Override
  public String toString() {
    return "CacheSlotMetadata{"
        + "cacheSlotState="
        + cacheSlotState
        + ", replicaId='"
        + replicaId
        + '\''
        + ", updatedTimeEpochMs="
        + updatedTimeEpochMs
        + ", supportedIndexTypes="
        + supportedIndexTypes
        + ", name='"
        + name
        + '\''
        + '}';
  }
}
