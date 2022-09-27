package com.slack.kaldb.metadata.replica;

import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LUCENE_REGULAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Instant;
import org.junit.Test;

public class ReplicaMetadataTest {

  @Test
  public void testReplicaMetadata() {
    String name = "name";
    String snapshotId = "snapshotId";
    long createdTimeEpochMs = Instant.now().toEpochMilli();
    long expireAfterEpochMs = Instant.now().toEpochMilli();

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            name, snapshotId, createdTimeEpochMs, expireAfterEpochMs, false, LUCENE_REGULAR);

    assertThat(replicaMetadata.name).isEqualTo(name);
    assertThat(replicaMetadata.snapshotId).isEqualTo(snapshotId);
    assertThat(replicaMetadata.createdTimeEpochMs).isEqualTo(createdTimeEpochMs);
    assertThat(replicaMetadata.expireAfterEpochMs).isEqualTo(expireAfterEpochMs);
    assertThat(replicaMetadata.isRestored).isFalse();
    assertThat(replicaMetadata.indexType).isEqualTo(LUCENE_REGULAR);

    ReplicaMetadata restoredReplicaMetadata =
        new ReplicaMetadata(
            name, snapshotId, createdTimeEpochMs, expireAfterEpochMs, true, LUCENE_REGULAR);

    assertThat(restoredReplicaMetadata.name).isEqualTo(name);
    assertThat(restoredReplicaMetadata.snapshotId).isEqualTo(snapshotId);
    assertThat(restoredReplicaMetadata.createdTimeEpochMs).isEqualTo(createdTimeEpochMs);
    assertThat(restoredReplicaMetadata.expireAfterEpochMs).isEqualTo(expireAfterEpochMs);
    assertThat(restoredReplicaMetadata.isRestored).isTrue();
    assertThat(restoredReplicaMetadata.indexType).isEqualTo(LUCENE_REGULAR);
  }

  @Test
  public void testReplicaMetadataEqualsHashcode() {
    String name = "name";
    String snapshotId = "snapshotId";
    long createdTimeEpochMs = Instant.now().toEpochMilli();
    long expireAfterEpochMs = Instant.now().toEpochMilli();

    ReplicaMetadata replicaMetadataA =
        new ReplicaMetadata(
            name, snapshotId, createdTimeEpochMs, expireAfterEpochMs, true, LUCENE_REGULAR);
    ReplicaMetadata replicaMetadataB =
        new ReplicaMetadata(
            name, snapshotId, createdTimeEpochMs, expireAfterEpochMs, true, LUCENE_REGULAR);
    ReplicaMetadata replicaMetadataC =
        new ReplicaMetadata(
            "nameC", snapshotId, createdTimeEpochMs, expireAfterEpochMs, true, LUCENE_REGULAR);
    ReplicaMetadata replicaMetadataD =
        new ReplicaMetadata(
            name, snapshotId, createdTimeEpochMs + 1, expireAfterEpochMs, false, LUCENE_REGULAR);
    ReplicaMetadata replicaMetadataE =
        new ReplicaMetadata(
            name, snapshotId, createdTimeEpochMs, expireAfterEpochMs + 1, false, LUCENE_REGULAR);

    assertThat(replicaMetadataA).isEqualTo(replicaMetadataB);
    assertThat(replicaMetadataA).isNotEqualTo(replicaMetadataC);
    assertThat(replicaMetadataA).isNotEqualTo(replicaMetadataD);
    assertThat(replicaMetadataA).isNotEqualTo(replicaMetadataE);

    assertThat(replicaMetadataA.hashCode()).isEqualTo(replicaMetadataB.hashCode());
    assertThat(replicaMetadataA.hashCode()).isNotEqualTo(replicaMetadataC.hashCode());
    assertThat(replicaMetadataA.hashCode()).isNotEqualTo(replicaMetadataD.hashCode());
    assertThat(replicaMetadataA.hashCode()).isNotEqualTo(replicaMetadataE.hashCode());
  }

  @Test
  public void invalidArgumentsShouldThrow() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ReplicaMetadata(
                    "name",
                    "",
                    Instant.now().toEpochMilli(),
                    Instant.now().toEpochMilli(),
                    false,
                    LUCENE_REGULAR));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ReplicaMetadata(
                    "name",
                    null,
                    Instant.now().toEpochMilli(),
                    Instant.now().toEpochMilli(),
                    true,
                    LUCENE_REGULAR));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ReplicaMetadata(
                    "name", "123", 0, Instant.now().toEpochMilli(), false, LUCENE_REGULAR));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ReplicaMetadata(
                    "name", "123", Instant.now().toEpochMilli(), -1, true, LUCENE_REGULAR));
  }
}
