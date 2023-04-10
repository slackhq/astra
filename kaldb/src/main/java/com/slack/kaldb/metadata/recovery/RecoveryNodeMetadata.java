package com.slack.kaldb.metadata.recovery;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.proto.metadata.Metadata;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The recovery node metadata is used to coordinate the availability of recovery node executors and
 * their recovery task assignments.
 */
public class RecoveryNodeMetadata extends KaldbMetadata {

  public final Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeState;
  public final String recoveryTaskName;
  public final List<Metadata.IndexType> supportedIndexTypes;
  public final long updatedTimeEpochMs;

  public RecoveryNodeMetadata(
      String name,
      Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeState,
      String recoveryTaskName,
      List<Metadata.IndexType> supportedIndexTypes,
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
    checkArgument(
        supportedIndexTypes != null && !supportedIndexTypes.isEmpty(),
        "supportedIndexTypes can't be null or empty");
    this.recoveryNodeState = recoveryNodeState;
    this.recoveryTaskName = recoveryTaskName;
    this.supportedIndexTypes = Collections.unmodifiableList(supportedIndexTypes);
    this.updatedTimeEpochMs = updatedTimeEpochMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RecoveryNodeMetadata)) return false;
    if (!super.equals(o)) return false;

    RecoveryNodeMetadata that = (RecoveryNodeMetadata) o;

    if (updatedTimeEpochMs != that.updatedTimeEpochMs) return false;
    if (recoveryNodeState != that.recoveryNodeState) return false;
    if (!Objects.equals(recoveryTaskName, that.recoveryTaskName)) return false;
    return Objects.equals(supportedIndexTypes, that.supportedIndexTypes);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (recoveryNodeState != null ? recoveryNodeState.hashCode() : 0);
    result = 31 * result + (recoveryTaskName != null ? recoveryTaskName.hashCode() : 0);
    result = 31 * result + (supportedIndexTypes != null ? supportedIndexTypes.hashCode() : 0);
    result = 31 * result + (int) (updatedTimeEpochMs ^ (updatedTimeEpochMs >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "RecoveryNodeMetadata{"
        + "recoveryNodeState="
        + recoveryNodeState
        + ", recoveryTaskName='"
        + recoveryTaskName
        + '\''
        + ", supportedIndexTypes="
        + supportedIndexTypes
        + ", updatedTimeEpochMs="
        + updatedTimeEpochMs
        + ", name='"
        + name
        + '\''
        + '}';
  }
}
