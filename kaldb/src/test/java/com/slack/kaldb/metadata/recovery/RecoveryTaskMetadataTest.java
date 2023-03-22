package com.slack.kaldb.metadata.recovery;

import static com.slack.kaldb.proto.metadata.Metadata.IndexType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Instant;
import org.junit.Test;

public class RecoveryTaskMetadataTest {

  @Test
  public void testRecoveryTaskMetadata() {
    String name = "name";
    String partitionId = "partitionId";
    long startOffset = 1;
    long endOffset = 2;
    long createdTimeEpochMs = 3;

    RecoveryTaskMetadata recoveryTaskMetadata =
        new RecoveryTaskMetadata(
            name, partitionId, startOffset, endOffset, IndexType.LOGS_LUCENE9, createdTimeEpochMs);

    assertThat(recoveryTaskMetadata.name).isEqualTo(name);
    assertThat(recoveryTaskMetadata.partitionId).isEqualTo(partitionId);
    assertThat(recoveryTaskMetadata.startOffset).isEqualTo(startOffset);
    assertThat(recoveryTaskMetadata.endOffset).isEqualTo(endOffset);
    assertThat(recoveryTaskMetadata.createdTimeEpochMs).isEqualTo(createdTimeEpochMs);
    assertThat(recoveryTaskMetadata.indexType).isEqualTo(IndexType.LOGS_LUCENE9);
  }

  @Test
  public void testRecoveryTaskMetadataEqualsHashcode() {
    String name = "name";
    String partitionId = "partitionId";
    long startOffset = 1;
    long endOffset = 5;
    long createdTimeEpochMs = 9;

    RecoveryTaskMetadata recoveryTaskMetadataA =
        new RecoveryTaskMetadata(
            name, partitionId, startOffset, endOffset, IndexType.LOGS_LUCENE9, createdTimeEpochMs);
    RecoveryTaskMetadata recoveryTaskMetadataB =
        new RecoveryTaskMetadata(
            name, partitionId, startOffset, endOffset, IndexType.LOGS_LUCENE9, createdTimeEpochMs);
    RecoveryTaskMetadata recoveryTaskMetadataC =
        new RecoveryTaskMetadata(
            name,
            "partitionIdC",
            startOffset,
            endOffset,
            IndexType.LOGS_LUCENE9,
            createdTimeEpochMs);
    RecoveryTaskMetadata recoveryTaskMetadataD =
        new RecoveryTaskMetadata(
            name, partitionId, 3, endOffset, IndexType.LOGS_LUCENE9, createdTimeEpochMs);
    RecoveryTaskMetadata recoveryTaskMetadataE =
        new RecoveryTaskMetadata(
            name, partitionId, startOffset, 8, IndexType.LOGS_LUCENE9, createdTimeEpochMs);
    RecoveryTaskMetadata recoveryTaskMetadataF =
            new RecoveryTaskMetadata(
                    name, partitionId, startOffset, 8, IndexType.UNRECOGNIZED, createdTimeEpochMs);

    // TODO: Add checks with different index type

    assertThat(recoveryTaskMetadataA).isEqualTo(recoveryTaskMetadataB);
    assertThat(recoveryTaskMetadataA).isNotEqualTo(recoveryTaskMetadataC);
    assertThat(recoveryTaskMetadataA).isNotEqualTo(recoveryTaskMetadataD);
    assertThat(recoveryTaskMetadataA).isNotEqualTo(recoveryTaskMetadataE);
    assertThat(recoveryTaskMetadataA).isNotEqualTo(recoveryTaskMetadataF);

    assertThat(recoveryTaskMetadataA.hashCode()).isEqualTo(recoveryTaskMetadataB.hashCode());
    assertThat(recoveryTaskMetadataA.hashCode()).isNotEqualTo(recoveryTaskMetadataC.hashCode());
    assertThat(recoveryTaskMetadataA.hashCode()).isNotEqualTo(recoveryTaskMetadataD.hashCode());
    assertThat(recoveryTaskMetadataA.hashCode()).isNotEqualTo(recoveryTaskMetadataE.hashCode());
    assertThat(recoveryTaskMetadataA.hashCode()).isNotEqualTo(recoveryTaskMetadataF.hashCode());
  }

  @Test
  public void testSingleMessageRecoveryTask() {
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata(
            "name", "partitionId", 10, 10, IndexType.LOGS_LUCENE9, Instant.now().toEpochMilli());
    assertThat(recoveryTask.startOffset).isEqualTo(recoveryTask.endOffset);

    RecoveryTaskMetadata recoveryTask2 =
        new RecoveryTaskMetadata(
            "name", "partitionId", 0, 0, IndexType.LOGS_LUCENE9, Instant.now().toEpochMilli());
    assertThat(recoveryTask2.startOffset).isEqualTo(recoveryTask2.endOffset);
  }

  @Test
  public void invalidArgumentsShouldThrow() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskMetadata(
                    "name", "", 0, 1, IndexType.LOGS_LUCENE9, Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskMetadata(
                    "name", null, 0, 1, IndexType.LOGS_LUCENE9, Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new RecoveryTaskMetadata("name", "partitionId", 0, 1, IndexType.LOGS_LUCENE9, 0));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskMetadata(
                    "name",
                    "partitionId",
                    Instant.now().toEpochMilli() + 10,
                    Instant.now().toEpochMilli() - 10,
                    IndexType.LOGS_LUCENE9,
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
            .isThrownBy(
                    () ->
                            new RecoveryTaskMetadata(
                                    "name",
                                    "partitionId",
                                    Instant.now().toEpochMilli() + 10,
                                    Instant.now().toEpochMilli() - 10,
                                    null,
                                    Instant.now().toEpochMilli()));
  }
}
