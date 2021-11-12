package com.slack.kaldb.metadata.recovery;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.Test;

public class RecoveryTaskMetadataTest {

  @Test
  public void testRecoveryTaskMetadata() {
    String name = "name";
    String partitionId = "partitionId";
    long startOffset = 1;
    long endOffset = 2;

    RecoveryTaskMetadata recoveryTaskMetadata =
        new RecoveryTaskMetadata(name, partitionId, startOffset, endOffset);

    assertThat(recoveryTaskMetadata.name).isEqualTo(name);
    assertThat(recoveryTaskMetadata.partitionId).isEqualTo(partitionId);
    assertThat(recoveryTaskMetadata.startOffset).isEqualTo(startOffset);
    assertThat(recoveryTaskMetadata.endOffset).isEqualTo(endOffset);
  }

  @Test
  public void testRecoveryTaskMetadataEqualsHashcode() {
    String name = "name";
    String partitionId = "partitionId";
    long startOffset = 1;
    long endOffset = 5;

    RecoveryTaskMetadata recoveryTaskMetadataA =
        new RecoveryTaskMetadata(name, partitionId, startOffset, endOffset);
    RecoveryTaskMetadata recoveryTaskMetadataB =
        new RecoveryTaskMetadata(name, partitionId, startOffset, endOffset);
    RecoveryTaskMetadata recoveryTaskMetadataC =
        new RecoveryTaskMetadata(name, "partitionIdC", startOffset, endOffset);
    RecoveryTaskMetadata recoveryTaskMetadataD =
        new RecoveryTaskMetadata(name, partitionId, 3, endOffset);
    RecoveryTaskMetadata recoveryTaskMetadataE =
        new RecoveryTaskMetadata(name, partitionId, startOffset, 8);

    assertThat(recoveryTaskMetadataA).isEqualTo(recoveryTaskMetadataB);
    assertThat(recoveryTaskMetadataA).isNotEqualTo(recoveryTaskMetadataC);
    assertThat(recoveryTaskMetadataA).isNotEqualTo(recoveryTaskMetadataD);
    assertThat(recoveryTaskMetadataA).isNotEqualTo(recoveryTaskMetadataE);

    assertThat(recoveryTaskMetadataA.hashCode()).isEqualTo(recoveryTaskMetadataB.hashCode());
    assertThat(recoveryTaskMetadataA.hashCode()).isNotEqualTo(recoveryTaskMetadataC.hashCode());
    assertThat(recoveryTaskMetadataA.hashCode()).isNotEqualTo(recoveryTaskMetadataD.hashCode());
    assertThat(recoveryTaskMetadataA.hashCode()).isNotEqualTo(recoveryTaskMetadataE.hashCode());
  }
}
