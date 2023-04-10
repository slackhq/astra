package com.slack.kaldb.metadata.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class RecoveryNodeMetadataTest {

  @Test
  public void testRecoveryNodeMetadata() {
    String name = "name";
    Metadata.RecoveryNodeMetadata.RecoveryNodeState state =
        Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED;
    String task = "task";
    long time = Instant.now().toEpochMilli();

    RecoveryNodeMetadata recoveryNodeMetadata =
        new RecoveryNodeMetadata(name, state, task, List.of(Metadata.IndexType.LOGS_LUCENE9), time);

    assertThat(recoveryNodeMetadata.name).isEqualTo(name);
    assertThat(recoveryNodeMetadata.recoveryNodeState).isEqualTo(state);
    assertThat(recoveryNodeMetadata.recoveryTaskName).isEqualTo(task);
    assertThat(recoveryNodeMetadata.supportedIndexTypes)
        .containsExactlyInAnyOrderElementsOf(List.of(Metadata.IndexType.LOGS_LUCENE9));
    assertThat(recoveryNodeMetadata.updatedTimeEpochMs).isEqualTo(time);
  }

  @Test
  public void testRecoveryNodeEqualsHashcode() {
    String name = "name";
    Metadata.RecoveryNodeMetadata.RecoveryNodeState state =
        Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED;
    String task = "task";
    long time = Instant.now().toEpochMilli();

    RecoveryNodeMetadata recoveryNodeMetadataA =
        new RecoveryNodeMetadata(name, state, task, List.of(Metadata.IndexType.LOGS_LUCENE9), time);
    RecoveryNodeMetadata recoveryNodeMetadataB =
        new RecoveryNodeMetadata(name, state, task, List.of(Metadata.IndexType.LOGS_LUCENE9), time);
    RecoveryNodeMetadata recoveryNodeMetadataC =
        new RecoveryNodeMetadata(
            name,
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING,
            task,
            List.of(Metadata.IndexType.LOGS_LUCENE9),
            time);
    RecoveryNodeMetadata recoveryNodeMetadataD =
        new RecoveryNodeMetadata(
            name, state, "taskD", List.of(Metadata.IndexType.LOGS_LUCENE9), time);
    RecoveryNodeMetadata recoveryNodeMetadataE =
        new RecoveryNodeMetadata(
            name, state, task, List.of(Metadata.IndexType.LOGS_LUCENE9), time + 1);
    RecoveryNodeMetadata recoveryNodeMetadataF =
        new RecoveryNodeMetadata(
            name,
            state,
            task,
            List.of(Metadata.IndexType.LOGS_LUCENE9, Metadata.IndexType.LOGS_LUCENE9),
            time + 1);

    assertThat(recoveryNodeMetadataA).isEqualTo(recoveryNodeMetadataB);
    assertThat(recoveryNodeMetadataA).isNotEqualTo(recoveryNodeMetadataC);
    assertThat(recoveryNodeMetadataA).isNotEqualTo(recoveryNodeMetadataD);
    assertThat(recoveryNodeMetadataA).isNotEqualTo(recoveryNodeMetadataE);
    assertThat(recoveryNodeMetadataA).isNotEqualTo(recoveryNodeMetadataF);
    assertThat(recoveryNodeMetadataE).isNotEqualTo(recoveryNodeMetadataF);

    assertThat(recoveryNodeMetadataA.hashCode()).isEqualTo(recoveryNodeMetadataB.hashCode());
    assertThat(recoveryNodeMetadataA.hashCode()).isNotEqualTo(recoveryNodeMetadataC.hashCode());
    assertThat(recoveryNodeMetadataA.hashCode()).isNotEqualTo(recoveryNodeMetadataD.hashCode());
    assertThat(recoveryNodeMetadataA.hashCode()).isNotEqualTo(recoveryNodeMetadataE.hashCode());
    assertThat(recoveryNodeMetadataA.hashCode()).isNotEqualTo(recoveryNodeMetadataF.hashCode());
    assertThat(recoveryNodeMetadataE.hashCode()).isNotEqualTo(recoveryNodeMetadataF.hashCode());
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
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryNodeMetadata(
                    "name",
                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
                    "",
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryNodeMetadata(
                    "name",
                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING,
                    "",
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryNodeMetadata(
                    "name",
                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
                    "123",
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    0));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryNodeMetadata(
                    "name",
                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
                    "123",
                    null,
                    1000));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryNodeMetadata(
                    "name",
                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
                    "123",
                    Collections.emptyList(),
                    1000));
  }
}
