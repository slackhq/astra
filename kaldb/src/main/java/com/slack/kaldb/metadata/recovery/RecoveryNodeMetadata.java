package com.slack.kaldb.metadata.recovery;

import static com.google.common.base.Preconditions.checkArgument;

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
  public final long updatedTimeEpochMs;

  public RecoveryNodeMetadata(
      String name,
      Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeState,
      String recoveryTaskName,
      long updatedTimeEpochMs) {
    super(name);

    checkArgument(updatedTimeEpochMs > 0, "Updated time must be greater than 0");
    if (recoveryNodeState.equals(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)) {
      checkArgument(
          recoveryTaskName != null && recoveryTaskName.isEmpty(),
          "Recovery task name must be empty if state is FREE");
    } else {
      checkArgument(
          recoveryTaskName != null && !recoveryTaskName.isEmpty(),
          "Recovery task name must not be empty if state is not FREE");
    }

    this.recoveryNodeState = recoveryNodeState;
    this.recoveryTaskName = recoveryTaskName;
    this.updatedTimeEpochMs = updatedTimeEpochMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    RecoveryNodeMetadata that = (RecoveryNodeMetadata) o;
    return updatedTimeEpochMs == that.updatedTimeEpochMs
        && recoveryNodeState == that.recoveryNodeState
        && recoveryTaskName.equals(that.recoveryTaskName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), recoveryNodeState, recoveryTaskName, updatedTimeEpochMs);
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
        + ", updatedTimeEpchMs="
        + updatedTimeEpochMs
        + '}';
  }
}
