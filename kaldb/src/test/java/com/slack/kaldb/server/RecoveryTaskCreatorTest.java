package com.slack.kaldb.server;

import static com.slack.kaldb.metadata.snapshot.SnapshotMetadata.LIVE_SNAPSHOT_PATH;
import static com.slack.kaldb.server.RecoveryTaskCreator.RECOVERY_TASKS_CREATED;
import static com.slack.kaldb.server.RecoveryTaskCreator.STALE_SNAPSHOT_DELETE_SUCCESS;
import static com.slack.kaldb.server.RecoveryTaskCreator.getHighestDurableOffsetForPartition;
import static com.slack.kaldb.server.RecoveryTaskCreator.getStaleLiveSnapshots;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage")
public class RecoveryTaskCreatorTest {
  private static final long TEST_MAX_MESSAGES_PER_RECOVERY_TASK = 10000;
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

    assertThat(getStaleLiveSnapshots(List.of(partition1), partitionId)).isEmpty();
    assertThat(getStaleLiveSnapshots(List.of(partition2), partitionId)).isEmpty();
    assertThat(getStaleLiveSnapshots(List.of(livePartition2), partitionId)).isEmpty();
    assertThat(getStaleLiveSnapshots(List.of(livePartition1), partitionId))
        .containsExactly(livePartition1);
    assertThat(getStaleLiveSnapshots(List.of(livePartition1, livePartition11), partitionId))
        .containsExactly(livePartition1, livePartition11);
    assertThat(getStaleLiveSnapshots(List.of(partition1, livePartition1), partitionId))
        .containsExactly(livePartition1);
    assertThat(getStaleLiveSnapshots(List.of(partition2, livePartition1), partitionId))
        .containsExactly(livePartition1);
    assertThat(getStaleLiveSnapshots(List.of(livePartition2, livePartition1), partitionId))
        .containsExactly(livePartition1);
    assertThat(
            getStaleLiveSnapshots(
                List.of(livePartition2, livePartition1, partition1, partition2), partitionId))
        .containsExactly(livePartition1);
    assertThat(
            getStaleLiveSnapshots(
                List.of(livePartition2, livePartition1, livePartition11, partition1, partition2),
                partitionId))
        .containsExactly(livePartition1, livePartition11);
    assertThat(getStaleLiveSnapshots(List.of(partition1, partition2), partitionId)).isEmpty();
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

    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            1,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);
    assertThat(recoveryTaskCreator.deleteStaleLiveSnapshots(actualSnapshots).size())
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

    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            1,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    // Throw exceptions on delete.
    doReturn(Futures.immediateFailedFuture(new RuntimeException()))
        .when(snapshotMetadataStore)
        .delete(any(SnapshotMetadata.class));

    if (hasException) {
      assertThatIllegalStateException()
          .isThrownBy(() -> recoveryTaskCreator.deleteStaleLiveSnapshots(actualSnapshots));
    } else {
      assertThat(recoveryTaskCreator.deleteStaleLiveSnapshots(actualSnapshots)).isEmpty();
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

    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            1,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

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
        .isThrownBy(() -> recoveryTaskCreator.deleteStaleLiveSnapshots(snapshots));

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
            getHighestDurableOffsetForPartition(
                Collections.emptyList(), Collections.emptyList(), partitionId))
        .isNegative();

    // Some snapshots, no recovery tasks.
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1), Collections.emptyList(), partitionId))
        .isEqualTo(maxOffset);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition2), Collections.emptyList(), partitionId))
        .isEqualTo(maxOffset);

    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11), Collections.emptyList(), partitionId))
        .isEqualTo(maxOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(maxOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition2, partition21), Collections.emptyList(), partitionId))
        .isEqualTo(maxOffset);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition12, partition2, partition21, partition22),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(maxOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition2), Collections.emptyList(), partitionId))
        .isNegative();
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition2, partition21, partition22),
                Collections.emptyList(),
                partitionId))
        .isNegative();
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(
                    partition1,
                    partition11,
                    partition12,
                    partition21,
                    partition2,
                    partition21,
                    partition22),
                Collections.emptyList(),
                partitionId))
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
            getHighestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask1), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask1, recoveryTask22), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask1, recoveryTask11), partitionId))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask11, recoveryTask21), partitionId))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(),
                List.of(recoveryTask1, recoveryTask11, recoveryTask21),
                partitionId))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(),
                List.of(recoveryTask1, recoveryTask11, recoveryTask21, recoveryTask22),
                partitionId))
        .isEqualTo(recoveryStartOffset * 3);

    //  snapshots and recovery tasks for same partition
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1), List.of(recoveryTask1), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11), List.of(recoveryTask1), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12), List.of(recoveryTask1), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12),
                List.of(recoveryTask1, recoveryTask11),
                partitionId))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition2), List.of(recoveryTask21), partitionId))
        .isNegative();
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition2, partition21),
                List.of(recoveryTask21, recoveryTask22),
                partitionId))
        .isNegative();

    //  snapshots for diff partitions, recovery tasks for diff partitions.
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition2, partition12), List.of(recoveryTask1), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition2, partition12), List.of(recoveryTask11), partitionId))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12, partition2, partition21, partition22),
                List.of(recoveryTask1, recoveryTask11, recoveryTask21, recoveryTask22),
                partitionId))
        .isEqualTo(recoveryStartOffset * 3);
  }

  @Test
  public void testInit() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskCreator(
                    snapshotMetadataStore,
                    recoveryTaskStore,
                    partitionId,
                    0,
                    TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
                    meterRegistry));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskCreator(
                    snapshotMetadataStore,
                    recoveryTaskStore,
                    "",
                    100,
                    TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
                    meterRegistry));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskCreator(
                    snapshotMetadataStore, recoveryTaskStore, partitionId, 100, 0, meterRegistry));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskCreator(
                    snapshotMetadataStore, recoveryTaskStore, partitionId, 100, -1, meterRegistry));
  }

  @Test
  public void testDetermineStartOffsetReturnsNegativeWhenNoOffset() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            1,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();
    assertThat(recoveryTaskCreator.determineStartingOffset(0)).isNegative();

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
    assertThat(recoveryTaskCreator.determineStartingOffset(0)).isNegative();

    final SnapshotMetadata partition11 =
        new SnapshotMetadata(name + "1", path, endTime + 1, endTime * 2, maxOffset * 2, "2");
    snapshotMetadataStore.createSync(partition11);
    assertThat(snapshotMetadataStore.listSync()).contains(partition1, partition11);
    assertThat(recoveryTaskCreator.determineStartingOffset(0)).isNegative();

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
    assertThat(recoveryTaskCreator.determineStartingOffset(0)).isNegative();
  }

  @Test
  public void testDetermineStartingOffsetOnlyRecoveryNotBehind() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();
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
    assertThat(recoveryTaskCreator.determineStartingOffset(850))
        .isEqualTo((recoveryStartOffset * 2) + 1);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(750));

    final RecoveryTaskMetadata recoveryTask11 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "11",
            partitionId,
            recoveryStartOffset * 2 + 1,
            recoveryStartOffset * 3,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask11);
    assertThat(recoveryTaskStore.listSync()).contains(recoveryTask1, recoveryTask11);
    assertThat(recoveryTaskCreator.determineStartingOffset(1201))
        .isEqualTo((recoveryStartOffset * 3) + 1);
    assertThat(recoveryTaskCreator.determineStartingOffset(1200))
        .isEqualTo((recoveryStartOffset * 3) + 1);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1150));
    assertThat(recoveryTaskStore.listSync()).contains(recoveryTask1, recoveryTask11);
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
  }

  @Test
  public void testDetermineStartingOffsetOnlyRecoveryBehind() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();
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
    assertThat(recoveryTaskCreator.determineStartingOffset(currentHeadOffset))
        .isEqualTo(currentHeadOffset);
    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskStore.listSync();
    assertThat(recoveryTasks.size()).isEqualTo(2);
    assertThat(recoveryTasks).contains(recoveryTask1);
    Optional<RecoveryTaskMetadata> newRecoveryTask =
        recoveryTasks.stream().filter(r -> !r.equals(recoveryTask1)).findFirst();
    assertThat(newRecoveryTask).isNotEmpty();
    RecoveryTaskMetadata recoveryTask = newRecoveryTask.get();
    assertThat(recoveryTask.startOffset).isEqualTo(recoveryStartOffset * 2 + 1);
    assertThat(recoveryTask.endOffset).isEqualTo(currentHeadOffset - 1);
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
  }

  @Test
  public void testDetermineStartingOffsetOnlyMultipleRecoveryBehind() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();
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
    assertThat(recoveryTaskCreator.determineStartingOffset(currentHeadOffset))
        .isEqualTo(currentHeadOffset);
    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskStore.listSync();
    assertThat(recoveryTasks.size()).isEqualTo(3);
    assertThat(recoveryTasks).contains(recoveryTask1, recoveryTask11);
    Optional<RecoveryTaskMetadata> newRecoveryTask =
        recoveryTasks.stream().filter(r -> !r.name.contains(recoveryTaskName)).findFirst();
    assertThat(newRecoveryTask).isNotEmpty();
    RecoveryTaskMetadata recoveryTask = newRecoveryTask.get();
    assertThat(recoveryTask.startOffset).isEqualTo(recoveryStartOffset * 3 + 1);
    assertThat(recoveryTask.endOffset).isEqualTo(currentHeadOffset - 1);
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
  }

  @Test
  public void testDetermineStartingOffsetMultiplePartitionRecoveriesBehind() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();
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
    final RecoveryTaskMetadata recoveryTask2 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "2",
            "2",
            recoveryStartOffset * 3 + 1,
            recoveryStartOffset * 4,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask2);
    final RecoveryTaskMetadata recoveryTask21 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "21", "2", recoveryStartOffset * 4 + 1, 50000, createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask21);
    assertThat(recoveryTaskStore.listSync())
        .contains(recoveryTask1, recoveryTask11, recoveryTask2, recoveryTask21);

    final long currentHeadOffset = 4000;
    assertThat(recoveryTaskCreator.determineStartingOffset(currentHeadOffset))
        .isEqualTo(currentHeadOffset);
    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskStore.listSync();
    assertThat(recoveryTasks.size()).isEqualTo(5);
    assertThat(recoveryTasks)
        .contains(recoveryTask1, recoveryTask11, recoveryTask2, recoveryTask21);
    Optional<RecoveryTaskMetadata> newRecoveryTask =
        recoveryTasks.stream().filter(r -> !r.name.contains(recoveryTaskName)).findFirst();
    assertThat(newRecoveryTask).isNotEmpty();
    RecoveryTaskMetadata recoveryTask = newRecoveryTask.get();
    assertThat(recoveryTask.startOffset).isEqualTo((recoveryStartOffset * 3) + 1);
    assertThat(recoveryTask.endOffset).isEqualTo(currentHeadOffset - 1);
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
  }

  @Test
  public void testDetermineStartingOffsetOnlySnapshotsNoDelay() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    snapshotMetadataStore.createSync(partition1);
    assertThat(snapshotMetadataStore.listSync()).contains(partition1);
    assertThat(
            getHighestDurableOffsetForPartition(
                snapshotMetadataStore.listSync(), Collections.emptyList(), partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(150)).isEqualTo(101);
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50));

    final SnapshotMetadata partition11 =
        new SnapshotMetadata(
            name + "11", path, endTime + 1, endTime * 2, maxOffset * 2, partitionId);

    snapshotMetadataStore.createSync(partition11);
    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(partition1, partition11);
    assertThat(recoveryTaskCreator.determineStartingOffset(250)).isEqualTo(201);
    assertThat(recoveryTaskCreator.determineStartingOffset(201)).isEqualTo(201);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(150));
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(0);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(0);

    // Live partition is cleaned up, no delay.
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "live1", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    snapshotMetadataStore.createSync(livePartition1);
    assertThat(snapshotMetadataStore.listSync()).contains(partition1, partition11, livePartition1);
    assertThat(recoveryTaskCreator.determineStartingOffset(250)).isEqualTo(201);
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(partition1, partition11);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(1);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(0);

    // Multiple live partitions for the same partition are cleaned up, no delay.
    snapshotMetadataStore.createSync(livePartition1);
    SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "live11", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    snapshotMetadataStore.createSync(livePartition11);
    assertThat(snapshotMetadataStore.listSync())
        .contains(partition1, partition11, livePartition1, livePartition11);
    assertThat(recoveryTaskCreator.determineStartingOffset(250)).isEqualTo(201);
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(partition1, partition11);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(3);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(0);

    // Live partitions from multiple stores exist.
    snapshotMetadataStore.createSync(livePartition1);
    snapshotMetadataStore.createSync(livePartition11);
    SnapshotMetadata livePartition2 =
        new SnapshotMetadata(
            name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset * 5, "2");
    snapshotMetadataStore.createSync(livePartition2);
    assertThat(snapshotMetadataStore.listSync())
        .contains(partition1, partition11, livePartition1, livePartition2);
    assertThat(recoveryTaskCreator.determineStartingOffset(250)).isEqualTo(201);
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    assertThat(snapshotMetadataStore.listSync())
        .containsExactlyInAnyOrder(partition1, partition11, livePartition2);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(5);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(0);

    // Live and non-live partitions for different partitions exist.
    snapshotMetadataStore.createSync(livePartition1);
    snapshotMetadataStore.createSync(livePartition11);
    SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "3", path, startTime, endTime, maxOffset * 3, "2");
    snapshotMetadataStore.createSync(partition2);
    assertThat(snapshotMetadataStore.listSync())
        .contains(partition1, partition11, livePartition1, livePartition2, partition2);
    assertThat(recoveryTaskCreator.determineStartingOffset(250)).isEqualTo(201);
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    assertThat(snapshotMetadataStore.listSync())
        .containsExactlyInAnyOrder(partition1, partition11, livePartition2, partition2);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(7);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(0);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testDetermineStartingOffsetOnlySnapshotsWithDelay() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    snapshotMetadataStore.createSync(partition1);
    assertThat(snapshotMetadataStore.listSync()).contains(partition1);
    assertThat(
            getHighestDurableOffsetForPartition(
                snapshotMetadataStore.listSync(), Collections.emptyList(), partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(1150)).isEqualTo(1150);
    List<RecoveryTaskMetadata> recoveryTasks1 = recoveryTaskStore.listSync();
    assertThat(recoveryTasks1.size()).isEqualTo(1);
    assertThat(recoveryTasks1.get(0).startOffset).isEqualTo(101);
    assertThat(recoveryTasks1.get(0).endOffset).isEqualTo(1149);
    assertThat(recoveryTasks1.get(0).partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50));
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(0);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(1);
    // clean up recovery task.
    recoveryTaskStore.deleteSync(recoveryTasks1.get(0).name);
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    final SnapshotMetadata partition11 =
        new SnapshotMetadata(
            name + "11", path, endTime + 1, endTime * 2, maxOffset * 2, partitionId);

    snapshotMetadataStore.createSync(partition11);
    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(partition1, partition11);
    assertThat(recoveryTaskCreator.determineStartingOffset(1250)).isEqualTo(1250);
    assertThat(recoveryTaskStore.listSync().size()).isEqualTo(1);
    RecoveryTaskMetadata recoveryTask1 = recoveryTaskStore.listSync().get(0);
    assertThat(recoveryTask1.startOffset).isEqualTo(201);
    assertThat(recoveryTask1.endOffset).isEqualTo(1249);
    assertThat(recoveryTask1.partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1248));
    assertThat(recoveryTaskCreator.determineStartingOffset(1249)).isEqualTo(1250);
    assertThat(recoveryTaskCreator.determineStartingOffset(1250)).isEqualTo(1250);
    assertThat(recoveryTaskCreator.determineStartingOffset(1251)).isEqualTo(1250);
    assertThat(recoveryTaskStore.listSync().size()).isEqualTo(1);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(0);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(2);

    // Live partition is cleaned up, new recovery task is created.
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "live1", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    snapshotMetadataStore.createSync(livePartition1);
    assertThat(recoveryTaskStore.listSync()).containsExactly(recoveryTask1);
    assertThat(snapshotMetadataStore.listSync()).contains(partition1, partition11, livePartition1);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(250));
    assertThat(recoveryTaskCreator.determineStartingOffset(1450)).isEqualTo(1450);
    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(partition1, partition11);
    List<RecoveryTaskMetadata> recoveryTasks2 = recoveryTaskStore.listSync();
    assertThat(recoveryTasks2.size()).isEqualTo(2);
    RecoveryTaskMetadata recoveryTask2 =
        recoveryTasks2.stream().filter(r -> !recoveryTask1.equals(r)).findFirst().get();
    assertThat(recoveryTask2.startOffset).isEqualTo(1250);
    assertThat(recoveryTask2.endOffset).isEqualTo(1449);
    assertThat(recoveryTask2.partitionId).isEqualTo(partitionId);
    assertThat(recoveryTaskStore.listSync())
        .containsExactlyInAnyOrder(recoveryTask1, recoveryTask2);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(1);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(3);

    // Multiple live partitions for the same partition are cleaned up.
    snapshotMetadataStore.createSync(livePartition1);
    SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "live11", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    snapshotMetadataStore.createSync(livePartition11);
    assertThat(snapshotMetadataStore.listSync())
        .contains(partition1, partition11, livePartition1, livePartition11);
    assertThat(recoveryTaskCreator.determineStartingOffset(1500)).isEqualTo(1450);
    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(partition1, partition11);
    assertThat(recoveryTaskStore.listSync()).contains(recoveryTask1, recoveryTask2);
    assertThat(recoveryTaskCreator.determineStartingOffset(1650)).isEqualTo(1650);
    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(partition1, partition11);
    List<RecoveryTaskMetadata> recoveryTasks3 = recoveryTaskStore.listSync();
    assertThat(recoveryTasks3.size()).isEqualTo(3);
    RecoveryTaskMetadata recoveryTask3 =
        recoveryTaskStore
            .listSync()
            .stream()
            .filter(r -> !r.equals(recoveryTask1) && !r.equals(recoveryTask2))
            .findFirst()
            .get();
    assertThat(recoveryTask3.partitionId).isEqualTo(partitionId);
    assertThat(recoveryTask3.startOffset).isEqualTo(1450);
    assertThat(recoveryTask3.endOffset).isEqualTo(1649);
    assertThat(recoveryTaskStore.listSync())
        .containsExactlyInAnyOrder(recoveryTask1, recoveryTask2, recoveryTask3);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(3);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(4);

    // Live partitions from multiple partitions exist.
    snapshotMetadataStore.createSync(livePartition1);
    snapshotMetadataStore.createSync(livePartition11);
    SnapshotMetadata livePartition2 =
        new SnapshotMetadata(
            name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset * 5, "2");
    snapshotMetadataStore.createSync(livePartition2);
    assertThat(snapshotMetadataStore.listSync())
        .contains(partition1, partition11, livePartition1, livePartition2);
    assertThat(recoveryTaskCreator.determineStartingOffset(1660)).isEqualTo(1650);
    assertThat(recoveryTaskStore.listSync())
        .containsExactlyInAnyOrder(recoveryTask1, recoveryTask2, recoveryTask3);
    assertThat(snapshotMetadataStore.listSync())
        .containsExactlyInAnyOrder(partition1, partition11, livePartition2);
    assertThat(recoveryTaskCreator.determineStartingOffset(1850)).isEqualTo(1850);
    List<RecoveryTaskMetadata> recoveryTasks4 = recoveryTaskStore.listSync();
    assertThat(recoveryTasks4.size()).isEqualTo(4);
    RecoveryTaskMetadata recoveryTask4 =
        recoveryTaskStore
            .listSync()
            .stream()
            .filter(r -> !recoveryTasks3.contains(r))
            .findFirst()
            .get();
    assertThat(recoveryTask4.partitionId).isEqualTo(partitionId);
    assertThat(recoveryTask4.startOffset).isEqualTo(1650);
    assertThat(recoveryTask4.endOffset).isEqualTo(1849);
    assertThat(snapshotMetadataStore.listSync())
        .containsExactlyInAnyOrder(partition1, partition11, livePartition2);
    assertThat(recoveryTaskStore.listSync())
        .containsExactlyInAnyOrder(recoveryTask1, recoveryTask2, recoveryTask3, recoveryTask4);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(5);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(5);

    // Live and non-live partitions for different partitions exist.
    snapshotMetadataStore.createSync(livePartition1);
    snapshotMetadataStore.createSync(livePartition11);
    SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "3", path, startTime, endTime, maxOffset * 3, "2");
    snapshotMetadataStore.createSync(partition2);
    assertThat(snapshotMetadataStore.listSync())
        .contains(partition1, partition11, livePartition1, livePartition2, partition2);
    final RecoveryTaskMetadata recoveryTaskPartition2 =
        new RecoveryTaskMetadata("basicRecovery" + "2", "2", 10000, 20000, 1000);
    recoveryTaskStore.createSync(recoveryTaskPartition2);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1650));
    assertThat(recoveryTaskCreator.determineStartingOffset(1900)).isEqualTo(1850);
    assertThat(recoveryTaskStore.listSync())
        .containsExactlyInAnyOrder(
            recoveryTask1, recoveryTask2, recoveryTask3, recoveryTask4, recoveryTaskPartition2);
    assertThat(recoveryTaskCreator.determineStartingOffset(2050)).isEqualTo(2050);
    assertThat(recoveryTaskStore.listSync().size()).isEqualTo(6);
    RecoveryTaskMetadata recoveryTask5 =
        recoveryTaskStore
            .listSync()
            .stream()
            .filter(r -> !recoveryTasks4.contains(r) && !r.equals(recoveryTaskPartition2))
            .findFirst()
            .get();
    assertThat(recoveryTask5.partitionId).isEqualTo(partitionId);
    assertThat(recoveryTask5.startOffset).isEqualTo(1850);
    assertThat(recoveryTask5.endOffset).isEqualTo(2049);
    assertThat(snapshotMetadataStore.listSync())
        .containsExactlyInAnyOrder(partition1, partition11, livePartition2, partition2);
    assertThat(recoveryTaskStore.listSync())
        .containsExactlyInAnyOrder(
            recoveryTask1,
            recoveryTask2,
            recoveryTask3,
            recoveryTask4,
            recoveryTask5,
            recoveryTaskPartition2);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(7);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(6);
  }

  @Test
  public void testRecoveryTaskCreationFailureFailsDetermineStartOffset() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    snapshotMetadataStore.createSync(partition1);
    assertThat(snapshotMetadataStore.listSync()).contains(partition1);
    assertThat(
            getHighestDurableOffsetForPartition(
                snapshotMetadataStore.listSync(), Collections.emptyList(), partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(1150)).isEqualTo(1150);
    List<RecoveryTaskMetadata> recoveryTasks1 = recoveryTaskStore.listSync();
    assertThat(recoveryTasks1.size()).isEqualTo(1);
    assertThat(recoveryTasks1.get(0).startOffset).isEqualTo(101);
    assertThat(recoveryTasks1.get(0).endOffset).isEqualTo(1149);
    assertThat(recoveryTasks1.get(0).partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50));

    // Fail a recovery store task creation.
    doThrow(new RuntimeException()).when(recoveryTaskStore).createSync(any());

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1350));
    assertThat(recoveryTaskStore.listSync()).containsExactly(recoveryTasks1.get(0));
    assertThat(snapshotMetadataStore.listSync()).contains(partition1);
  }

  @Test
  public void testSnapshotListFailureFailsDetermineStartOffset()
      throws ExecutionException, InterruptedException {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    snapshotMetadataStore.createSync(partition1);
    assertThat(snapshotMetadataStore.listSync()).contains(partition1);
    assertThat(
            getHighestDurableOffsetForPartition(
                snapshotMetadataStore.listSync(), Collections.emptyList(), partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(1150)).isEqualTo(1150);
    List<RecoveryTaskMetadata> recoveryTasks1 = recoveryTaskStore.listSync();
    assertThat(recoveryTasks1.size()).isEqualTo(1);
    assertThat(recoveryTasks1.get(0).startOffset).isEqualTo(101);
    assertThat(recoveryTasks1.get(0).endOffset).isEqualTo(1149);
    assertThat(recoveryTasks1.get(0).partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50));

    // Fail snapshot store list creation.
    doThrow(new RuntimeException()).when(snapshotMetadataStore).listSync();

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1350));
    assertThat(recoveryTaskStore.listSync()).containsExactly(recoveryTasks1.get(0));
    assertThat(snapshotMetadataStore.list().get()).containsExactly(partition1);
  }

  @Test
  public void testRecoveryListFailureFailsDetermineStartOffset()
      throws ExecutionException, InterruptedException {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    snapshotMetadataStore.createSync(partition1);
    assertThat(snapshotMetadataStore.listSync()).contains(partition1);
    assertThat(
            getHighestDurableOffsetForPartition(
                snapshotMetadataStore.listSync(), Collections.emptyList(), partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(1150)).isEqualTo(1150);
    List<RecoveryTaskMetadata> recoveryTasks1 = recoveryTaskStore.listSync();
    assertThat(recoveryTasks1.size()).isEqualTo(1);
    assertThat(recoveryTasks1.get(0).startOffset).isEqualTo(101);
    assertThat(recoveryTasks1.get(0).endOffset).isEqualTo(1149);
    assertThat(recoveryTasks1.get(0).partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50));

    // Fail a recovery store list.
    doThrow(new RuntimeException()).when(recoveryTaskStore).listSync();

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1350));
    assertThat(recoveryTaskStore.list().get()).containsExactly(recoveryTasks1.get(0));
    assertThat(snapshotMetadataStore.list().get()).containsExactly(partition1);
  }

  @Test
  public void testFailureToDeleteStaleSnapshotsFailsDetermineStartOffset()
      throws ExecutionException, InterruptedException {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    snapshotMetadataStore.createSync(partition1);
    assertThat(snapshotMetadataStore.listSync()).contains(partition1);
    assertThat(
            getHighestDurableOffsetForPartition(
                snapshotMetadataStore.listSync(), Collections.emptyList(), partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(1150)).isEqualTo(1150);
    List<RecoveryTaskMetadata> recoveryTasks1 = recoveryTaskStore.listSync();
    assertThat(recoveryTasks1.size()).isEqualTo(1);
    assertThat(recoveryTasks1.get(0).startOffset).isEqualTo(101);
    assertThat(recoveryTasks1.get(0).endOffset).isEqualTo(1149);
    assertThat(recoveryTasks1.get(0).partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50));

    // Add a live partition to be deleted.
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "1", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    snapshotMetadataStore.createSync(livePartition1);
    assertThat(snapshotMetadataStore.list().get())
        .containsExactlyInAnyOrder(partition1, livePartition1);
    // Fail deletion on snapshot metadata store.
    doThrow(new IllegalStateException())
        .when(snapshotMetadataStore)
        .delete(any(SnapshotMetadata.class));
    doThrow(new IllegalStateException()).when(snapshotMetadataStore).delete(any(String.class));

    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1350));
    assertThat(recoveryTaskStore.list().get()).containsExactly(recoveryTasks1.get(0));
    assertThat(snapshotMetadataStore.list().get())
        .containsExactlyInAnyOrder(partition1, livePartition1);
  }

  @Test
  public void testChunkingRecoveryTaskLongRange() {
    final long maxMessagesPerRecoveryTask = 100;
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            maxMessagesPerRecoveryTask,
            meterRegistry);

    // Long range - 1 chunk
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    recoveryTaskCreator.createRecoveryTasks("1", 10, 100, maxMessagesPerRecoveryTask);
    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskStore.listSync();
    assertThat(recoveryTasks.size()).isEqualTo(1);
    assertThat(recoveryTasks.get(0).startOffset).isEqualTo(10);
    assertThat(recoveryTasks.get(0).endOffset).isEqualTo(100);
    assertThat((recoveryTasks.get(0)).partitionId).isEqualTo(partitionId);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void testChunkingRecoveryOneChunk() {
    final long maxMessagesPerRecoveryTask = 100;
    final String testPartitionId = "2";
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            testPartitionId,
            100,
            maxMessagesPerRecoveryTask,
            meterRegistry);

    assertThat(recoveryTaskStore.listSync()).isEmpty();
    recoveryTaskCreator.createRecoveryTasks(testPartitionId, 100, 101, maxMessagesPerRecoveryTask);
    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskStore.listSync();
    assertThat(recoveryTasks.size()).isEqualTo(1);
    assertThat(recoveryTasks.get(0).startOffset).isEqualTo(100);
    assertThat(recoveryTasks.get(0).endOffset).isEqualTo(101);
    assertThat((recoveryTasks.get(0)).partitionId).isEqualTo(testPartitionId);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void testChunkingRecoveryTasksSameSizeTasks() {
    final String testPartitionId = "3";
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore, recoveryTaskStore, testPartitionId, 100, 2, meterRegistry);

    assertThat(recoveryTaskStore.listSync()).isEmpty();
    recoveryTaskCreator.createRecoveryTasks(testPartitionId, 100, 105, 2);
    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskStore.listSync();
    assertThat(recoveryTasks.size()).isEqualTo(3);
    assertThat(recoveryTasks.stream().mapToLong(r -> r.startOffset).sorted().toArray())
        .containsExactly(100, 102, 104);
    assertThat(recoveryTasks.stream().mapToLong(r -> r.endOffset).sorted().toArray())
        .containsExactly(101, 103, 105);
    assertThat(recoveryTasks.stream().mapToLong(r -> r.endOffset - r.startOffset).toArray())
        .containsExactly(1, 1, 1);
    assertThat(recoveryTasks.stream().filter(r -> r.partitionId.equals(testPartitionId)).count())
        .isEqualTo(3);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(3);
  }

  @Test
  public void testChunkingRecoveryTasksWithOddSizeTask() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore, recoveryTaskStore, partitionId, 100, 2, meterRegistry);

    assertThat(recoveryTaskStore.listSync()).isEmpty();
    recoveryTaskCreator.createRecoveryTasks(partitionId, 100, 104, 2);
    List<RecoveryTaskMetadata> recoveryTasks = recoveryTaskStore.listSync();
    assertThat(recoveryTasks.size()).isEqualTo(3);
    assertThat(recoveryTasks.stream().mapToLong(r -> r.startOffset).sorted().toArray())
        .containsExactly(100, 102, 104);
    assertThat(recoveryTasks.stream().mapToLong(r -> r.endOffset).sorted().toArray())
        .containsExactly(101, 103, 104);
    assertThat(
            recoveryTasks.stream().mapToLong(r -> r.endOffset - r.startOffset).sorted().toArray())
        .containsExactly(0, 1, 1);
    assertThat(recoveryTasks.stream().filter(r -> r.partitionId.equals(partitionId)).count())
        .isEqualTo(3);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(3);
  }

  @Test
  public void testMultipleRecoveryTaskCreationWithSnapshotDelay() {
    final long maxMessagesPerRecoveryTask = 500;
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            maxMessagesPerRecoveryTask,
            meterRegistry);

    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000)).isNegative();
    // assertThat(recoveryTaskStore.listSync()).isEmpty();

    //    final String name = "testSnapshotId";
    //    final String path = "/testPath_" + name;
    //    final long startTime = 1;
    //    final long endTime = 100;
    //    final long maxOffset = 100;
    //
    //    final SnapshotMetadata partition1 =
    //        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    //    snapshotMetadataStore.createSync(partition1);
    //    assertThat(snapshotMetadataStore.listSync()).contains(partition1);
    //    assertThat(
    //            getHighestDurableOffsetForPartition(
    //                snapshotMetadataStore.listSync(), Collections.emptyList(), partitionId))
    //        .isEqualTo(100);
    //    assertThat(recoveryTaskCreator.determineStartingOffset(1150)).isEqualTo(1150);
    //    List<RecoveryTaskMetadata> recoveryTasks1 = recoveryTaskStore.listSync();
    //    assertThat(recoveryTasks1.size()).isEqualTo(3);
    //    assertThat(recoveryTasks1.stream().mapToLong(r -> r.startOffset).sorted().toArray())
    //        .containsExactly(101, 601, 1101);
    //    assertThat(recoveryTasks1.stream().mapToLong(r -> r.endOffset).sorted().toArray())
    //        .containsExactly(600, 1100, 1149);
    //    assertThat(
    //            recoveryTasks1.stream().mapToLong(r -> r.endOffset -
    // r.startOffset).sorted().toArray())
    //        .containsExactly(48, 499, 499);
    //    assertThat(recoveryTasks1.stream().filter(r -> r.partitionId.equals(partitionId)).count())
    //        .isEqualTo(3);
    //    assertThatIllegalStateException()
    //        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50));
    //    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(0);
    //    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(3);
  }
}
