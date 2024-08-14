package com.slack.astra.metadata.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class SnapshotMetadataTest {
  @Test
  public void testSnapshotMetadata() {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(name, startTime, endTime, maxOffset, partitionId, 0);

    assertThat(snapshotMetadata.name).isEqualTo(name);
    assertThat(snapshotMetadata.snapshotId).isEqualTo(name);
    assertThat(snapshotMetadata.startTimeEpochMs).isEqualTo(startTime);
    assertThat(snapshotMetadata.endTimeEpochMs).isEqualTo(endTime);
    assertThat(snapshotMetadata.maxOffset).isEqualTo(maxOffset);
    assertThat(snapshotMetadata.partitionId).isEqualTo(partitionId);
  }

  @Test
  public void testEqualsAndHashCode() {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 0;
    final String partitionId = "1";

    SnapshotMetadata snapshot1 =
        new SnapshotMetadata(name, startTime, endTime, maxOffset, partitionId, 0);
    SnapshotMetadata snapshot2 =
        new SnapshotMetadata(name + "2", startTime, endTime, maxOffset, partitionId, 0);

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
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SnapshotMetadata("", startTime, endTime, maxOffset, partitionId, 0));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SnapshotMetadata(name, 0, endTime, maxOffset, partitionId, 0));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SnapshotMetadata(name, startTime, 0, maxOffset, partitionId, 0));

    // Start time < end time
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new SnapshotMetadata(name, endTime, startTime, maxOffset, partitionId, 0));

    // Start time same as end time.
    assertThat(
            new SnapshotMetadata(name, startTime, startTime, maxOffset, partitionId, 0)
                .endTimeEpochMs)
        .isEqualTo(startTime);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SnapshotMetadata(name, startTime, endTime, -1, partitionId, 0));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SnapshotMetadata(name, startTime, endTime, maxOffset, "", 0));
  }

  @Test
  public void testLive() {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    SnapshotMetadata nonLiveSnapshot =
        new SnapshotMetadata(name, startTime, endTime, maxOffset, partitionId, 100);
    assertThat(nonLiveSnapshot.isLive()).isFalse();

    SnapshotMetadata liveSnapshot =
        new SnapshotMetadata(name, startTime, endTime, maxOffset, partitionId, 0);
    assertThat(liveSnapshot.isLive()).isTrue();
  }
}
