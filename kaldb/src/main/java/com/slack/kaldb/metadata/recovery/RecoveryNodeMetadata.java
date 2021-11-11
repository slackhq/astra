package com.slack.kaldb.metadata.recovery;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.proto.metadata.Metadata;
import java.util.Objects;

/**
 * The recovery node metadata is used to coordinate the availability of recovery node executors and
 * their recovery task assignments.
 */
public class RecoveryNodeMetadata extends KaldbMetadata {

  public final Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeState;
  public final String recoveryTaskName;
  public final long updatedTimeUtc;

  public RecoveryNodeMetadata(
      String name,
      Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeState,
      String recoveryTaskName,
      long updatedTimeUtc) {
    super(name);
    this.recoveryNodeState = recoveryNodeState;
    this.recoveryTaskName = recoveryTaskName;
    this.updatedTimeUtc = updatedTimeUtc;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    RecoveryNodeMetadata that = (RecoveryNodeMetadata) o;
    return updatedTimeUtc == that.updatedTimeUtc
        && recoveryNodeState == that.recoveryNodeState
        && recoveryTaskName.equals(that.recoveryTaskName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), recoveryNodeState, recoveryTaskName, updatedTimeUtc);
  }

  @Override
  public String toString() {
    return "RecoveryNodeMetadata{"
        + "name='"
        + name
        + '\''
        + ", recoveryNodeState="
        + recoveryNodeState
        + ", recoveryTaskName='"
        + recoveryTaskName
        + '\''
        + ", updatedTimeUtc="
        + updatedTimeUtc
        + '}';
  }
}
