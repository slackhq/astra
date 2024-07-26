package com.slack.astra.metadata.cache;

import com.slack.astra.metadata.core.AstraMetadata;
import java.util.Objects;

public class CacheNodeMetadata extends AstraMetadata {
  public final String id;
  public final String hostname;
  public final long nodeCapacityBytes;
  public final String replicaSet;

  public CacheNodeMetadata(String id, String hostname, long nodeCapacityBytes, String replicaSet) {
    super(id);
    this.id = id;
    this.hostname = hostname;
    this.nodeCapacityBytes = nodeCapacityBytes;
    this.replicaSet = replicaSet;
  }

  public String getReplicaSet() {
    return replicaSet;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CacheNodeMetadata that)) return false;
    if (!super.equals(o)) return false;

    if (!hostname.equals(that.hostname)) return false;
    if (!Objects.equals(replicaSet, that.replicaSet)) return false;
    if (nodeCapacityBytes != that.nodeCapacityBytes) return false;
    return id.equals(that.id);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + hostname.hashCode();
    result = 31 * result + (replicaSet != null ? replicaSet.hashCode() : 0);
    result = 31 * result + id.hashCode();
    result = 31 * result + Long.hashCode(nodeCapacityBytes);
    return result;
  }

  @Override
  public String toString() {
    return "CacheNodeMetadata{"
        + "id='"
        + id
        + '\''
        + ", hostname='"
        + hostname
        + '\''
        + ", replicaSet="
        + replicaSet
        + ", nodeCapacityBytes='"
        + nodeCapacityBytes
        + '\''
        + '}';
  }
}
