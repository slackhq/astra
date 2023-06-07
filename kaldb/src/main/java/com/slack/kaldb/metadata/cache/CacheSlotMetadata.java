package com.slack.kaldb.metadata.cache;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.proto.metadata.Metadata;
import java.util.Collections;
import java.util.List;

/**
 * TODO: Currently, application code directly manipulates cache slot states which is error prone.
 * Make transitions more controlled via a state machine like API.
 */
public class CacheSlotMetadata extends KaldbMetadata {
  public final String hostname;
  public final Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState;
  public final String replicaId;
  public final long updatedTimeEpochMs;
  public final List<Metadata.IndexType> supportedIndexTypes;

  public CacheSlotMetadata(
      String name,
      Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState,
      String replicaId,
      long updatedTimeEpochMs,
      List<Metadata.IndexType> supportedIndexTypes,
      String hostname) {
    super(name);
    checkArgument(hostname != null && !hostname.isEmpty(), "Hostname cannot be null or empty");
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

    this.hostname = hostname;
    this.cacheSlotState = cacheSlotState;
    this.replicaId = replicaId;
    this.updatedTimeEpochMs = updatedTimeEpochMs;
    this.supportedIndexTypes = Collections.unmodifiableList(supportedIndexTypes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CacheSlotMetadata that)) return false;
    if (!super.equals(o)) return false;

    if (updatedTimeEpochMs != that.updatedTimeEpochMs) return false;
    if (!hostname.equals(that.hostname)) return false;
    if (cacheSlotState != that.cacheSlotState) return false;
    if (!replicaId.equals(that.replicaId)) return false;
    return supportedIndexTypes.equals(that.supportedIndexTypes);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + hostname.hashCode();
    result = 31 * result + cacheSlotState.hashCode();
    result = 31 * result + replicaId.hashCode();
    result = 31 * result + (int) (updatedTimeEpochMs ^ (updatedTimeEpochMs >>> 32));
    result = 31 * result + supportedIndexTypes.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "CacheSlotMetadata{"
        + "hostname='"
        + hostname
        + '\''
        + ", cacheSlotState="
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
