package com.slack.kaldb.metadata.replica;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.Test;

public class ReplicaMetadataTest {

  @Test
  public void testReplicaMetadata() {
    String name = "name";
    String snapshotId = "snapshotId";

    ReplicaMetadata replicaMetadata = new ReplicaMetadata(name, snapshotId);

    assertThat(replicaMetadata.name).isEqualTo(name);
    assertThat(replicaMetadata.snapshotId).isEqualTo(snapshotId);
  }

  @Test
  public void testReplicaMetadataEqualsHashcode() {
    String name = "name";
    String snapshotId = "snapshotId";

    ReplicaMetadata replicaMetadataA = new ReplicaMetadata(name, snapshotId);
    ReplicaMetadata replicaMetadataB = new ReplicaMetadata(name, snapshotId);
    ReplicaMetadata replicaMetadataC = new ReplicaMetadata("nameC", snapshotId);

    assertThat(replicaMetadataA).isEqualTo(replicaMetadataB);
    assertThat(replicaMetadataA).isNotEqualTo(replicaMetadataC);

    assertThat(replicaMetadataA.hashCode()).isEqualTo(replicaMetadataB.hashCode());
    assertThat(replicaMetadataA.hashCode()).isNotEqualTo(replicaMetadataC.hashCode());
  }
}
