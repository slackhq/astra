package com.slack.astra.metadata.replica;

import static com.slack.astra.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Instant;
import org.junit.jupiter.api.Test;

public class ReplicaMetadataTest {

  @Test
  public void testReplicaMetadata() {
    String name = "name";
    String snapshotId = "snapshotId";
    String replicaSet = "rep1";
    long createdTimeEpochMs = Instant.now().toEpochMilli();
    long expireAfterEpochMs = Instant.now().toEpochMilli();

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            name,
            snapshotId,
            replicaSet,
            createdTimeEpochMs,
            expireAfterEpochMs,
            false,
            LOGS_LUCENE9);

    assertThat(replicaMetadata.name).isEqualTo(name);
    assertThat(replicaMetadata.snapshotId).isEqualTo(snapshotId);
    assertThat(replicaMetadata.replicaSet).isEqualTo(replicaSet);
    assertThat(replicaMetadata.createdTimeEpochMs).isEqualTo(createdTimeEpochMs);
    assertThat(replicaMetadata.expireAfterEpochMs).isEqualTo(expireAfterEpochMs);
    assertThat(replicaMetadata.isRestored).isFalse();
    assertThat(replicaMetadata.indexType).isEqualTo(LOGS_LUCENE9);

    ReplicaMetadata restoredReplicaMetadata =
        new ReplicaMetadata(
            name,
            snapshotId,
            replicaSet,
            createdTimeEpochMs,
            expireAfterEpochMs,
            true,
            LOGS_LUCENE9);

    assertThat(restoredReplicaMetadata.name).isEqualTo(name);
    assertThat(restoredReplicaMetadata.snapshotId).isEqualTo(snapshotId);
    assertThat(restoredReplicaMetadata.replicaSet).isEqualTo(replicaSet);
    assertThat(restoredReplicaMetadata.createdTimeEpochMs).isEqualTo(createdTimeEpochMs);
    assertThat(restoredReplicaMetadata.expireAfterEpochMs).isEqualTo(expireAfterEpochMs);
    assertThat(restoredReplicaMetadata.isRestored).isTrue();
    assertThat(restoredReplicaMetadata.indexType).isEqualTo(LOGS_LUCENE9);
  }

  @Test
  public void testReplicaMetadataEqualsHashcode() {
    String name = "name";
    String snapshotId = "snapshotId";
    String replicaSet = "rep1";
    long createdTimeEpochMs = Instant.now().toEpochMilli();
    long expireAfterEpochMs = Instant.now().toEpochMilli();

    ReplicaMetadata replicaMetadataA =
        new ReplicaMetadata(
            name,
            snapshotId,
            replicaSet,
            createdTimeEpochMs,
            expireAfterEpochMs,
            true,
            LOGS_LUCENE9);
    ReplicaMetadata replicaMetadataB =
        new ReplicaMetadata(
            name,
            snapshotId,
            replicaSet,
            createdTimeEpochMs,
            expireAfterEpochMs,
            true,
            LOGS_LUCENE9);
    ReplicaMetadata replicaMetadataC =
        new ReplicaMetadata(
            "nameC",
            snapshotId,
            replicaSet,
            createdTimeEpochMs,
            expireAfterEpochMs,
            true,
            LOGS_LUCENE9);
    ReplicaMetadata replicaMetadataD =
        new ReplicaMetadata(
            name,
            snapshotId,
            replicaSet,
            createdTimeEpochMs + 1,
            expireAfterEpochMs,
            false,
            LOGS_LUCENE9);
    ReplicaMetadata replicaMetadataE =
        new ReplicaMetadata(
            name,
            snapshotId,
            replicaSet,
            createdTimeEpochMs,
            expireAfterEpochMs + 1,
            false,
            LOGS_LUCENE9);
    ReplicaMetadata replicaMetadataF =
        new ReplicaMetadata(
            name, snapshotId, "rep2", createdTimeEpochMs, expireAfterEpochMs, true, LOGS_LUCENE9);

    assertThat(replicaMetadataA).isEqualTo(replicaMetadataB);
    assertThat(replicaMetadataA).isNotEqualTo(replicaMetadataC);
    assertThat(replicaMetadataA).isNotEqualTo(replicaMetadataD);
    assertThat(replicaMetadataA).isNotEqualTo(replicaMetadataE);
    assertThat(replicaMetadataA).isNotEqualTo(replicaMetadataF);

    assertThat(replicaMetadataA.hashCode()).isEqualTo(replicaMetadataB.hashCode());
    assertThat(replicaMetadataA.hashCode()).isNotEqualTo(replicaMetadataC.hashCode());
    assertThat(replicaMetadataA.hashCode()).isNotEqualTo(replicaMetadataD.hashCode());
    assertThat(replicaMetadataA.hashCode()).isNotEqualTo(replicaMetadataE.hashCode());
    assertThat(replicaMetadataA.hashCode()).isNotEqualTo(replicaMetadataF.hashCode());
  }

  @Test
  public void invalidArgumentsShouldThrow() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ReplicaMetadata(
                    "name",
                    "",
                    "rep1",
                    Instant.now().toEpochMilli(),
                    Instant.now().toEpochMilli(),
                    false,
                    LOGS_LUCENE9));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ReplicaMetadata(
                    "name",
                    null,
                    "rep1",
                    Instant.now().toEpochMilli(),
                    Instant.now().toEpochMilli(),
                    true,
                    LOGS_LUCENE9));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ReplicaMetadata(
                    "name", "123", "rep1", 0, Instant.now().toEpochMilli(), false, LOGS_LUCENE9));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ReplicaMetadata(
                    "name", "123", "rep1", Instant.now().toEpochMilli(), -1, true, LOGS_LUCENE9));
  }
}
