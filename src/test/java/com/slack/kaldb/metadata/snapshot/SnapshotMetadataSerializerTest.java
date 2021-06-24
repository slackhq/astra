package com.slack.kaldb.metadata.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SnapshotMetadataSerializerTest {

  @Test
  public void testSnapshotMetadataSerializer() {
    final String snapshotName = "testSnapshot";
    final String snapshotPath = "/testPath_" + snapshotName;
    final String snapshotId = snapshotName + "_id";
    final long snapshotStartTime = 1;
    final long snapshotEndTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            snapshotName,
            snapshotPath,
            snapshotId,
            snapshotStartTime,
            snapshotEndTime,
            maxOffset,
            partitionId);

    assertThat(snapshotMetadata.name).isEqualTo(snapshotName);
  }
}
