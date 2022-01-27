package com.slack.kaldb.server;

import static com.slack.kaldb.metadata.snapshot.SnapshotMetadata.LIVE_SNAPSHOT_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.google.common.util.concurrent.Futures;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage")
public class RecoveryTaskFactoryTest {
  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl zkMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private RecoveryTaskMetadataStore recoveryTaskStore;
  private static final String partitionId = "1";

  @Before
  public void startup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();
    zkMetadataStore =
        new ZookeeperMetadataStoreImpl(
            testingServer.getConnectString(),
            "test",
            1000,
            1000,
            new RetryNTimes(1, 500),
            new CountingFatalErrorHandler(),
            meterRegistry);
    snapshotMetadataStore = spy(new SnapshotMetadataStore(zkMetadataStore, false));
    recoveryTaskStore = spy(new RecoveryTaskMetadataStore(zkMetadataStore, false));
  }

  @After
  public void shutdown() throws IOException {
    recoveryTaskStore.close();
    snapshotMetadataStore.close();
    zkMetadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testStaleSnapshotDetection() {
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;

    SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "1", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "11", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata livePartition2 =
        new SnapshotMetadata(name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, "2");
    SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "3", path, startTime, endTime, maxOffset, "2");

    RecoveryTaskFactory recoveryTaskFactory =
        new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, partitionId, 1);

    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(partition1))).isEmpty();
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(partition2))).isEmpty();
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(livePartition2))).isEmpty();
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(livePartition1)))
        .containsExactly(livePartition1);
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(livePartition1, livePartition11)))
        .containsExactly(livePartition1, livePartition11);
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(partition1, livePartition1)))
        .containsExactly(livePartition1);
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(partition2, livePartition1)))
        .containsExactly(livePartition1);
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(livePartition2, livePartition1)))
        .containsExactly(livePartition1);
    assertThat(
            recoveryTaskFactory.getStaleLiveSnapshots(
                List.of(livePartition2, livePartition1, partition1, partition2)))
        .containsExactly(livePartition1);
    assertThat(
            recoveryTaskFactory.getStaleLiveSnapshots(
                List.of(livePartition2, livePartition1, livePartition11, partition1, partition2)))
        .containsExactly(livePartition1, livePartition11);
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(partition1, partition2)))
        .isEmpty();
  }

  @Test
  public void testDeleteStaleSnapshotDeletion() {
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;

    SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "1", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "11", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata livePartition2 =
        new SnapshotMetadata(name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, "2");
    SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "3", path, startTime, endTime, maxOffset, "2");

    testDeleteSnapshots(List.of(partition1), 0, List.of(partition1));
    testDeleteSnapshots(List.of(partition2), 0, List.of(partition2));
    testDeleteSnapshots(List.of(livePartition2), 0, List.of(livePartition2));
    testDeleteSnapshots(List.of(livePartition1), 1, Collections.emptyList());
    testDeleteSnapshots(List.of(livePartition1, livePartition11), 2, Collections.emptyList());
    testDeleteSnapshots(List.of(partition1, livePartition1), 1, List.of(partition1));
    testDeleteSnapshots(
        List.of(partition1, livePartition1, livePartition11), 2, List.of(partition1));
    testDeleteSnapshots(
        List.of(partition2, livePartition2), 0, List.of(partition2, livePartition2));
    testDeleteSnapshots(List.of(partition2, partition1), 0, List.of(partition2, partition1));
    testDeleteSnapshots(
        List.of(partition2, partition1, livePartition11), 1, List.of(partition2, partition1));
    testDeleteSnapshots(
        List.of(partition1, livePartition1, partition2, livePartition2),
        1,
        List.of(partition2, partition1, livePartition2));
    testDeleteSnapshots(
        List.of(partition1, livePartition1, livePartition11, partition2, livePartition2),
        2,
        List.of(partition2, partition1, livePartition2));
  }

  private void testDeleteSnapshots(
      List<SnapshotMetadata> actualSnapshots,
      int deletedSnapshotSize,
      List<SnapshotMetadata> expectedSnapshots) {
    actualSnapshots.forEach(snapshot -> snapshotMetadataStore.createSync(snapshot));
    assertThat(snapshotMetadataStore.listSync())
        .containsExactlyInAnyOrderElementsOf(actualSnapshots);

    RecoveryTaskFactory recoveryTaskFactory =
        new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, partitionId, 1);
    assertThat(recoveryTaskFactory.deleteStaleLiveSnapsnots(actualSnapshots).size())
        .isEqualTo(deletedSnapshotSize);
    assertThat(snapshotMetadataStore.listSync())
        .containsExactlyInAnyOrderElementsOf(expectedSnapshots);
    // Clear state
    expectedSnapshots.forEach(snapshot -> snapshotMetadataStore.deleteSync(snapshot));
  }

  @Test
  public void shouldStaleDeletionShouldHandleTimeouts() {
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;

    SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "1", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "11", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata livePartition2 =
        new SnapshotMetadata(name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, "2");
    SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "3", path, startTime, endTime, maxOffset, "2");

    testDeleteSnapshotsTimeouts(List.of(partition1), List.of(partition1), false);
    testDeleteSnapshotsTimeouts(List.of(livePartition1), List.of(livePartition1), true);
    testDeleteSnapshotsTimeouts(
        List.of(partition1, livePartition1), List.of(partition1, livePartition1), true);
    testDeleteSnapshotsTimeouts(
        List.of(partition1, livePartition1, livePartition11),
        List.of(partition1, livePartition1, livePartition11),
        true);
    testDeleteSnapshotsTimeouts(
        List.of(partition1, livePartition1, livePartition11, partition2),
        List.of(partition1, livePartition1, livePartition11, partition2),
        true);
    testDeleteSnapshotsTimeouts(
        List.of(partition1, livePartition1, livePartition11, partition2, livePartition2),
        List.of(partition1, livePartition1, livePartition11, partition2, livePartition2),
        true);
  }

  private void testDeleteSnapshotsTimeouts(
      List<SnapshotMetadata> actualSnapshots,
      List<SnapshotMetadata> expectedSnapshots,
      boolean hasException) {

    actualSnapshots.forEach(snapshot -> snapshotMetadataStore.createSync(snapshot));
    assertThat(snapshotMetadataStore.listSync())
        .containsExactlyInAnyOrderElementsOf(actualSnapshots);

    RecoveryTaskFactory recoveryTaskFactory =
        new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, partitionId, 1);

    // Throw exceptions on delete.
    doReturn(Futures.immediateFailedFuture(new RuntimeException()))
        .when(snapshotMetadataStore)
        .delete((any(SnapshotMetadata.class)));

    if (hasException) {
      assertThatIllegalStateException()
          .isThrownBy(() -> recoveryTaskFactory.deleteStaleLiveSnapsnots(actualSnapshots));
    } else {
      assertThat(recoveryTaskFactory.deleteStaleLiveSnapsnots(actualSnapshots)).isEmpty();
    }

    assertThat(snapshotMetadataStore.listSync())
        .containsExactlyInAnyOrderElementsOf(expectedSnapshots);

    // Clear state but reset the overloaded method.
    doCallRealMethod().when(snapshotMetadataStore).delete((SnapshotMetadata) any());
    expectedSnapshots.forEach(snapshot -> snapshotMetadataStore.deleteSync(snapshot));
  }

  @Test
  public void shouldStaleDeletionShouldHandleExceptions() {
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    final SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "1", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    final SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "11", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    final SnapshotMetadata livePartition2 =
        new SnapshotMetadata(name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, "2");
    final SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "3", path, startTime, endTime, maxOffset, "2");

    List<SnapshotMetadata> snapshots =
        List.of(partition1, livePartition1, livePartition11, partition2, livePartition2);

    snapshots.forEach(snapshot -> snapshotMetadataStore.createSync(snapshot));
    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrderElementsOf(snapshots);

    RecoveryTaskFactory recoveryTaskFactory =
        new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, partitionId, 1);

    // Pass first call, fail second call.
    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();
    // allow the first deletion to work, and timeout the second one
    doCallRealMethod()
        .doReturn(
            Futures.submit(
                () -> {
                  try {
                    Thread.sleep(30 * 1000);
                  } catch (InterruptedException ignored) {
                  }
                },
                timeoutServiceExecutor))
        .when(snapshotMetadataStore)
        .delete(any(SnapshotMetadata.class));

    doCallRealMethod()
        .doReturn(Futures.immediateFailedFuture(new RuntimeException()))
        .when(snapshotMetadataStore)
        .delete((SnapshotMetadata) any());

    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskFactory.deleteStaleLiveSnapsnots(snapshots));

    // Either liveSnapshot1 or liveSnapshot11 remain but not both.
    List<SnapshotMetadata> actualSnapshots = snapshotMetadataStore.listSync();
    assertThat(actualSnapshots.size()).isEqualTo(4);
    assertThat(actualSnapshots).contains(partition1, partition2, livePartition2);
    assertThat(
            (actualSnapshots.contains(livePartition1) && !actualSnapshots.contains(livePartition11))
                || (actualSnapshots.contains(livePartition11)
                    && !actualSnapshots.contains(livePartition1)))
        .isTrue();
  }

  @Test
  public void testMaxOffset() {
    RecoveryTaskFactory recoveryTaskFactory =
        new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, partitionId, 1);

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    final SnapshotMetadata partition11 =
        new SnapshotMetadata(
            name + "1", path, endTime + 1, endTime * 2, maxOffset * 2, partitionId);
    final SnapshotMetadata partition12 =
        new SnapshotMetadata(
            name + "12", path, endTime * 2 + 1, endTime * 3, maxOffset * 3, partitionId);

    final String partitionId2 = "2";
    final long partition2Offset = maxOffset * 10;
    final SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "2", path, startTime, endTime, partition2Offset, partitionId2);
    final SnapshotMetadata partition21 =
        new SnapshotMetadata(
            name + "21", path, endTime + 1, endTime * 2, partition2Offset * 2, partitionId2);
    final SnapshotMetadata partition22 =
        new SnapshotMetadata(
            name + "22", path, endTime * 2 + 1, endTime * 3, partition2Offset * 3, partitionId2);

    // empty results
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                Collections.emptyList(), Collections.emptyList()))
        .isNegative();

    // Some snapshots, no recovery tasks.
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1), Collections.emptyList()))
        .isEqualTo(maxOffset);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1, partition2), Collections.emptyList()))
        .isEqualTo(maxOffset);

    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1, partition11), Collections.emptyList()))
        .isEqualTo(maxOffset * 2);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12), Collections.emptyList()))
        .isEqualTo(maxOffset * 3);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1, partition2, partition21), Collections.emptyList()))
        .isEqualTo(maxOffset);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition12, partition2, partition21, partition22),
                Collections.emptyList()))
        .isEqualTo(maxOffset * 3);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition2), Collections.emptyList()))
        .isNegative();
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition2, partition21, partition22), Collections.emptyList()))
        .isNegative();
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(
                    partition1,
                    partition11,
                    partition12,
                    partition21,
                    partition2,
                    partition21,
                    partition22),
                Collections.emptyList()))
        .isEqualTo(maxOffset * 3);

    // Only recovery tasks, no snapshots.
    final String recoveryTaskName = "recoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            partitionId,
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    final RecoveryTaskMetadata recoveryTask11 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "11",
            partitionId,
            recoveryStartOffset * 2 + 1,
            recoveryStartOffset * 3,
            createdTimeUtc);
    final RecoveryTaskMetadata recoveryTask21 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "21",
            partitionId2,
            recoveryStartOffset * 5 + 1,
            recoveryStartOffset * 6,
            createdTimeUtc);
    final RecoveryTaskMetadata recoveryTask22 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "21",
            partitionId2,
            recoveryStartOffset * 6 + 1,
            recoveryStartOffset * 7,
            createdTimeUtc);

    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask1)))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask1, recoveryTask22)))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask1, recoveryTask11)))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask11, recoveryTask21)))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask1, recoveryTask11, recoveryTask21)))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                Collections.emptyList(),
                List.of(recoveryTask1, recoveryTask11, recoveryTask21, recoveryTask22)))
        .isEqualTo(recoveryStartOffset * 3);

    //  snapshots and recovery tasks for same partition
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1), List.of(recoveryTask1)))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1, partition11), List.of(recoveryTask1)))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12), List.of(recoveryTask1)))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12),
                List.of(recoveryTask1, recoveryTask11)))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition2), List.of(recoveryTask21)))
        .isNegative();
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition2, partition21), List.of(recoveryTask21, recoveryTask22)))
        .isNegative();

    //  snapshots for diff partitions, recovery tasks for diff partitions.
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1, partition2, partition12), List.of(recoveryTask1)))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1, partition2, partition12), List.of(recoveryTask11)))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            recoveryTaskFactory.getHigestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12, partition2, partition21, partition22),
                List.of(recoveryTask1, recoveryTask11, recoveryTask21, recoveryTask22)))
        .isEqualTo(recoveryStartOffset * 3);
  }

  @Test
  public void testInit() {
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, partitionId, 0));

    assertThatIllegalStateException()
        .isThrownBy(
            () -> new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, "", 100));
  }

  @Test
  public void testDetermineStartOffsetReturnsNegativeWhenNoOffset() {
    RecoveryTaskFactory recoveryTaskFactory =
        new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, partitionId, 1);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskFactory.determineStartingOffset(1000)).isNegative();
    assertThat(recoveryTaskFactory.determineStartingOffset(0)).isNegative();

    // Data exists for not for this partition.
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, "2");
    snapshotMetadataStore.createSync(partition1);
    assertThat(snapshotMetadataStore.listSync()).contains(partition1);
    assertThat(recoveryTaskFactory.determineStartingOffset(0)).isNegative();

    final SnapshotMetadata partition11 =
        new SnapshotMetadata(name + "1", path, endTime + 1, endTime * 2, maxOffset * 2, "2");
    snapshotMetadataStore.createSync(partition11);
    assertThat(snapshotMetadataStore.listSync()).contains(partition1, partition11);
    assertThat(recoveryTaskFactory.determineStartingOffset(0)).isNegative();

    final String recoveryTaskName = "recoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            "2",
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask1);
    assertThat(recoveryTaskStore.listSync()).contains(recoveryTask1);
    assertThat(recoveryTaskFactory.determineStartingOffset(0)).isNegative();
  }

  @Test
  public void testDetermineStartingOffsetOnlyRecoveryNotBehind() {
    RecoveryTaskFactory recoveryTaskFactory =
        new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, partitionId, 100);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskFactory.determineStartingOffset(1000)).isNegative();
    final String recoveryTaskName = "recoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            partitionId,
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask1);
    assertThat(recoveryTaskStore.listSync()).contains(recoveryTask1);
    assertThat(recoveryTaskFactory.determineStartingOffset(850)).isEqualTo(recoveryStartOffset * 2);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskFactory.determineStartingOffset(750));

    final RecoveryTaskMetadata recoveryTask11 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "11",
            partitionId,
            recoveryStartOffset * 2 + 1,
            recoveryStartOffset * 3,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask11);
    assertThat(recoveryTaskStore.listSync()).contains(recoveryTask1, recoveryTask11);
    assertThat(recoveryTaskFactory.determineStartingOffset(1201))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(recoveryTaskFactory.determineStartingOffset(1200))
        .isEqualTo(recoveryStartOffset * 3);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskFactory.determineStartingOffset(1150));
    assertThat(recoveryTaskStore.listSync()).contains(recoveryTask1, recoveryTask11);
  }

  @Test
  public void testDetermineStartingOffsetOnlyRecoveryBehind() {
    RecoveryTaskFactory recoveryTaskFactory =
        new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, partitionId, 100);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskFactory.determineStartingOffset(1000)).isNegative();
    final String recoveryTaskName = "recoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            partitionId,
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask1);
    assertThat(recoveryTaskStore.listSync()).contains(recoveryTask1);
    final long currentHeadOffset = 4000;
    assertThat(recoveryTaskFactory.determineStartingOffset(currentHeadOffset))
        .isEqualTo(currentHeadOffset);
    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskStore.listSync();
    assertThat(recoveryTasks.size()).isEqualTo(2);
    assertThat(recoveryTasks).contains(recoveryTask1);
    Optional<RecoveryTaskMetadata> newRecoveryTask =
        recoveryTasks.stream().filter(r -> !r.equals(recoveryTask1)).findFirst();
    assertThat(newRecoveryTask).isNotEmpty();
    if (newRecoveryTask.isPresent()) {
      RecoveryTaskMetadata recoveryTask = newRecoveryTask.get();
      assertThat(recoveryTask.startOffset).isEqualTo(recoveryStartOffset * 2);
      assertThat(recoveryTask.endOffset).isEqualTo(currentHeadOffset);
    }
  }

  @Test
  public void testDetermineStartingOffsetOnlyMultipleRecoveryBehind() {
    RecoveryTaskFactory recoveryTaskFactory =
        new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, partitionId, 100);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskFactory.determineStartingOffset(1000)).isNegative();
    final String recoveryTaskName = "BasicRecoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            partitionId,
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask1);
    final RecoveryTaskMetadata recoveryTask11 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "11",
            partitionId,
            recoveryStartOffset * 2 + 1,
            recoveryStartOffset * 3,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask11);
    assertThat(recoveryTaskStore.listSync()).contains(recoveryTask1, recoveryTask11);

    final long currentHeadOffset = 4000;
    assertThat(recoveryTaskFactory.determineStartingOffset(currentHeadOffset))
        .isEqualTo(currentHeadOffset);
    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskStore.listSync();
    assertThat(recoveryTasks.size()).isEqualTo(3);
    assertThat(recoveryTasks).contains(recoveryTask1, recoveryTask11);
    Optional<RecoveryTaskMetadata> newRecoveryTask =
        recoveryTasks.stream().filter(r -> !r.name.contains(recoveryTaskName)).findFirst();
    assertThat(newRecoveryTask).isNotEmpty();
    if (newRecoveryTask.isPresent()) {
      RecoveryTaskMetadata recoveryTask = newRecoveryTask.get();
      assertThat(recoveryTask.startOffset).isEqualTo(recoveryStartOffset * 3);
      assertThat(recoveryTask.endOffset).isEqualTo(currentHeadOffset);
    }
  }

  @Test
  public void testDetermineStartingOffsetOnlySnapshotsNotBehind() {}

  // TODO: Test determine start offset.
  // only snapshots, no recovery.
  // only snapshots, multiple recoveries.

  // only snapshots, no recovery, behind.
  // only snapshots, no recovery, not behind.
  // only snapshots, multiple recoveries, behind.
  // only snapshots, multiple recoveries, not behind.
  // Test with delay of zero.

  // Throw exception in these cases.
  // only snapshots, no recovery, behind.
  // only snapshots, no recovery, not behind.
  // only snapshots, multiple recoveries, behind
  // only snapshots, multiple recoveries, not behind.

  // TODO: Test reliability and fault tolerance across multiple restarts of indexer. Double
  //  recovery task creation.
}
