package com.slack.kaldb.metadata.cache;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.proto.metadata.Metadata;

public class CacheSlotMetadata extends KaldbMetadata {
  public static final String METADATA_SLOT_NAME = "SLOT";

  public final Metadata.CacheSlotState cacheSlotState;
  public final String replicaId;
  public final long updatedTimeUtc;

  public CacheSlotMetadata(
      String name, Metadata.CacheSlotState cacheSlotState, String replicaId, long updatedTimeUtc) {
    super(name);
    this.cacheSlotState = cacheSlotState;
    this.replicaId = replicaId;
    this.updatedTimeUtc = updatedTimeUtc;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    CacheSlotMetadata that = (CacheSlotMetadata) o;

    if (!cacheSlotState.equals(that.cacheSlotState)) return false;
    if (updatedTimeUtc != that.updatedTimeUtc) return false;
    return replicaId.equals(that.replicaId);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + cacheSlotState.hashCode();
    result = 31 * result + replicaId.hashCode();
    result = 31 * result + (int) (updatedTimeUtc ^ (updatedTimeUtc >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "CacheNodeMetadata{"
        + "name='"
        + name
        + '\''
        + ", cacheSlotState='"
        + cacheSlotState
        + '\''
        + ", replicaId='"
        + replicaId
        + '\''
        + ", updatedTimeUtc='"
        + updatedTimeUtc
        + '\''
        + '}';
  }
}
