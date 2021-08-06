package com.slack.kaldb.metadata.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class SnapshotMetadataTest {
  @Test
  public void testSnapshotMetadata() {
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
    assertThat(snapshotMetadata.snapshotPath).isEqualTo(path);
    assertThat(snapshotMetadata.snapshotId).isEqualTo(id);
    assertThat(snapshotMetadata.startTimeUtc).isEqualTo(startTime);
    assertThat(snapshotMetadata.endTimeUtc).isEqualTo(endTime);
    assertThat(snapshotMetadata.maxOffset).isEqualTo(maxOffset);
    assertThat(snapshotMetadata.partitionId).isEqualTo(partitionId);
  }

  @Test
  public void testEqualsAndHashCode() {
    final String name = "testSnapshot";
    final String path = "/testPath_" + name;
    final String id = name + "_id";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    SnapshotMetadata snapshot1 =
        new SnapshotMetadata(name, path, id, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata snapshot2 =
        new SnapshotMetadata(name + "2", path, id, startTime, endTime, maxOffset, partitionId);

    assertThat(snapshot1).isEqualTo(snapshot1);
    // Ensure the name field from super class is included.
    assertThat(snapshot1).isNotEqualTo(snapshot2);
    Set<SnapshotMetadata> set = new HashSet<>();
    set.add(snapshot1);
    set.add(snapshot2);
    assertThat(set.size()).isEqualTo(2);
    assertThat(Map.of("1", snapshot1, "2", snapshot2).size()).isEqualTo(2);
  }

  @Test
  public void ensureValidSnapshotData() {
    final String name = "testSnapshot";
    final String path = "/testPath_" + name;
    final String id = name + "_id";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    assertThatIllegalStateException()
        .isThrownBy(
            () -> new SnapshotMetadata("", path, id, startTime, endTime, maxOffset, partitionId));

    assertThatIllegalStateException()
        .isThrownBy(
            () -> new SnapshotMetadata(name, "", id, startTime, endTime, maxOffset, partitionId));

    assertThatIllegalStateException()
        .isThrownBy(
            () -> new SnapshotMetadata(name, path, "", startTime, endTime, maxOffset, partitionId));

    assertThatIllegalStateException()
        .isThrownBy(() -> new SnapshotMetadata(name, path, id, 0, endTime, maxOffset, partitionId));

    assertThatIllegalStateException()
        .isThrownBy(
            () -> new SnapshotMetadata(name, path, id, startTime, 0, maxOffset, partitionId));

    // Start time < end time
    assertThatIllegalStateException()
        .isThrownBy(
            () -> new SnapshotMetadata(name, path, id, endTime, startTime, maxOffset, partitionId));

    assertThatIllegalStateException()
        .isThrownBy(
            () -> new SnapshotMetadata(name, path, id, startTime, endTime, -1, partitionId));

    assertThatIllegalStateException()
        .isThrownBy(() -> new SnapshotMetadata(name, path, id, startTime, endTime, maxOffset, ""));
  }
}
