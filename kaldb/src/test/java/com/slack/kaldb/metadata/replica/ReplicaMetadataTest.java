package com.slack.kaldb.metadata.replica;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Instant;
import org.junit.Test;

public class ReplicaMetadataTest {

  @Test
  public void testReplicaMetadata() {
    String name = "name";
    String snapshotId = "snapshotId";
    long createdTimeUtc = Instant.now().toEpochMilli();
    long expireAfterUtc = Instant.now().toEpochMilli();

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(name, snapshotId, createdTimeUtc, expireAfterUtc);

    assertThat(replicaMetadata.name).isEqualTo(name);
    assertThat(replicaMetadata.snapshotId).isEqualTo(snapshotId);
    assertThat(replicaMetadata.createdTimeEpochMs).isEqualTo(createdTimeUtc);
  }

  @Test
  public void testReplicaMetadataEqualsHashcode() {
    String name = "name";
    String snapshotId = "snapshotId";
    long createdTimeUtc = Instant.now().toEpochMilli();
    long expireAfterUtc = Instant.now().toEpochMilli();

    ReplicaMetadata replicaMetadataA =
        new ReplicaMetadata(name, snapshotId, createdTimeUtc, expireAfterUtc);
    ReplicaMetadata replicaMetadataB =
        new ReplicaMetadata(name, snapshotId, createdTimeUtc, expireAfterUtc);
    ReplicaMetadata replicaMetadataC =
        new ReplicaMetadata("nameC", snapshotId, createdTimeUtc, expireAfterUtc);
    ReplicaMetadata replicaMetadataD =
        new ReplicaMetadata(name, snapshotId, createdTimeUtc + 1, expireAfterUtc);
    ReplicaMetadata replicaMetadataE =
        new ReplicaMetadata(name, snapshotId, createdTimeUtc, expireAfterUtc + 1);

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
