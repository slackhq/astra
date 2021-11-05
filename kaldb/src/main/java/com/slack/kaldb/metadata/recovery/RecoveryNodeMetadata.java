package com.slack.kaldb.metadata.recovery;

import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.proto.metadata.Metadata;
import java.util.Objects;

public class RecoveryNodeMetadata extends KaldbMetadata {

  public final Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeState;

  public RecoveryNodeMetadata(
      String name, Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeState) {
    super(name);
    this.recoveryNodeState = recoveryNodeState;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    RecoveryNodeMetadata that = (RecoveryNodeMetadata) o;
    return recoveryNodeState == that.recoveryNodeState;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), recoveryNodeState);
  }

  @Override
  public String toString() {
    return "RecoveryNodeMetadata{"
        + "name='"
        + name
        + '\''
        + ", recoveryNodeState="
        + recoveryNodeState
        + '}';
  }
}
