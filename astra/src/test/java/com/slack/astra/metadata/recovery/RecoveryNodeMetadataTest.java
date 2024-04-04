package com.slack.astra.metadata.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.slack.astra.proto.metadata.Metadata;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class RecoveryNodeMetadataTest {

  @Test
  public void testRecoveryNodeMetadata() {
    String name = "name";
    Metadata.RecoveryNodeMetadata.RecoveryNodeState state =
        Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED;
    String task = "task";
    long time = Instant.now().toEpochMilli();

    RecoveryNodeMetadata recoveryNodeMetadata = new RecoveryNodeMetadata(name, state, task, time);

    assertThat(recoveryNodeMetadata.name).isEqualTo(name);
    assertThat(recoveryNodeMetadata.recoveryNodeState).isEqualTo(state);
    assertThat(recoveryNodeMetadata.recoveryTaskName).isEqualTo(task);
    assertThat(recoveryNodeMetadata.updatedTimeEpochMs).isEqualTo(time);
  }

  @Test
  public void testRecoveryNodeEqualsHashcode() {
    String name = "name";
    Metadata.RecoveryNodeMetadata.RecoveryNodeState state =
        Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED;
    String task = "task";
    long time = Instant.now().toEpochMilli();

    RecoveryNodeMetadata recoveryNodeMetadataA = new RecoveryNodeMetadata(name, state, task, time);
    RecoveryNodeMetadata recoveryNodeMetadataB = new RecoveryNodeMetadata(name, state, task, time);
    RecoveryNodeMetadata recoveryNodeMetadataC =
        new RecoveryNodeMetadata(
            name, Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING, task, time);
    RecoveryNodeMetadata recoveryNodeMetadataD =
        new RecoveryNodeMetadata(name, state, "taskD", time);
    RecoveryNodeMetadata recoveryNodeMetadataE =
        new RecoveryNodeMetadata(name, state, task, time + 1);

    assertThat(recoveryNodeMetadataA).isEqualTo(recoveryNodeMetadataB);
    assertThat(recoveryNodeMetadataA).isNotEqualTo(recoveryNodeMetadataC);
    assertThat(recoveryNodeMetadataA).isNotEqualTo(recoveryNodeMetadataD);
    assertThat(recoveryNodeMetadataA).isNotEqualTo(recoveryNodeMetadataE);

    assertThat(recoveryNodeMetadataA.hashCode()).isEqualTo(recoveryNodeMetadataB.hashCode());
    assertThat(recoveryNodeMetadataA.hashCode()).isNotEqualTo(recoveryNodeMetadataC.hashCode());
    assertThat(recoveryNodeMetadataA.hashCode()).isNotEqualTo(recoveryNodeMetadataD.hashCode());
    assertThat(recoveryNodeMetadataA.hashCode()).isNotEqualTo(recoveryNodeMetadataE.hashCode());
  }

  @Test
  public void invalidArgumentsShouldThrow() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryNodeMetadata(
                    "name",
                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
                    "123",
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryNodeMetadata(
                    "name",
                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
                    "",
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryNodeMetadata(
                    "name",
                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING,
                    "",
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryNodeMetadata(
                    "name", Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED, "123", 0));
  }
}
