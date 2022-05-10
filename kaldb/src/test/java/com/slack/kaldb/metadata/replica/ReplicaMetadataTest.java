package com.slack.kaldb.metadata.replica;

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
        new ReplicaMetadata(name, snapshotId, createdTimeEpochMs, expireAfterEpochMs);

    assertThat(replicaMetadata.name).isEqualTo(name);
    assertThat(replicaMetadata.snapshotId).isEqualTo(snapshotId);
    assertThat(replicaMetadata.createdTimeEpochMs).isEqualTo(createdTimeEpochMs);
  }

  @Test
  public void testReplicaMetadataEqualsHashcode() {
    String name = "name";
    String snapshotId = "snapshotId";
    long createdTimeEpochMs = Instant.now().toEpochMilli();
    long expireAfterEpochMs = Instant.now().toEpochMilli();

    ReplicaMetadata replicaMetadataA =
        new ReplicaMetadata(name, snapshotId, createdTimeEpochMs, expireAfterEpochMs);
    ReplicaMetadata replicaMetadataB =
        new ReplicaMetadata(name, snapshotId, createdTimeEpochMs, expireAfterEpochMs);
    ReplicaMetadata replicaMetadataC =
        new ReplicaMetadata("nameC", snapshotId, createdTimeEpochMs, expireAfterEpochMs);
    ReplicaMetadata replicaMetadataD =
        new ReplicaMetadata(name, snapshotId, createdTimeEpochMs + 1, expireAfterEpochMs);
    ReplicaMetadata replicaMetadataE =
        new ReplicaMetadata(name, snapshotId, createdTimeEpochMs, expireAfterEpochMs + 1);

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
                    "name", "", Instant.now().toEpochMilli(), Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ReplicaMetadata(
                    "name", null, Instant.now().toEpochMilli(), Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ReplicaMetadata("name", "123", 0, Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ReplicaMetadata("name", "123", Instant.now().toEpochMilli(), -1));
  }
}
