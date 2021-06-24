package com.slack.kaldb.metadata.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SnapshotMetadataSerializerTest {

  @Test
  public void testSnapshotMetadataSerializer() {
    final String name = "testSnapshot";
    final String path = "/testPath_" + name;
    final String id = name + "_id";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(name, path, id, startTime, endTime, maxOffset, partitionId);

    assertThat(snapshotMetadata.name).isEqualTo(name);
  }
}
