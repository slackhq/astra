package com.slack.kaldb.chunk;

import static com.slack.kaldb.chunk.ChunkInfo.MAX_FUTURE_TIME;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.testlib.ChunkManagerUtil.TEST_HOST;
import static com.slack.kaldb.testlib.ChunkManagerUtil.TEST_PORT;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchLiveSnapshot;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchNonLiveSnapshot;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchSearchNodes;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchSnapshots;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.chunkManager.ChunkCleanerService;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.assertj.core.data.Offset;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class ChunkCleanerServiceTest {
  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static final String TEST_KAFKA_PARTITION_ID = "10";
  private SimpleMeterRegistry metricsRegistry;
  private ChunkManagerUtil<LogMessage> chunkManagerUtil;

  @Before
  public void setUp() throws Exception {
    KaldbConfigUtil.initEmptyIndexerConfig();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        new ChunkManagerUtil<>(S3_MOCK_RULE, metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);
  }

  @After
  public void tearDown() throws IOException, TimeoutException {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
    metricsRegistry.close();
    KaldbConfig.reset();
  }

  @Test
  public void testDeleteStaleDataOn1Chunk()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    final Instant creationTime = Instant.now();
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, Duration.ofSeconds(100));
    assertThat(chunkManager.getChunkList().isEmpty()).isTrue();
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 9, 1000, startTime);

    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(9);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);

    ReadWriteChunkImpl<LogMessage> chunk = chunkManager.getActiveChunk();
    assertThat(chunk.isReadOnly()).isFalse();
    assertThat(chunk.info().getChunkSnapshotTimeEpochMs()).isZero();

    testBasicSnapshotMetadata(chunkManager, creationTime);

    // Commit the chunk and roll it over.
    chunkManager.rollOverActiveChunk();
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(9);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);

    List<SnapshotMetadata> afterSnapshots = fetchSnapshots(chunkManager);
    assertThat(afterSnapshots.size()).isEqualTo(2);
    assertThat(afterSnapshots).contains(ChunkInfo.toSnapshotMetadata(chunk.info(), ""));
    SnapshotMetadata liveSnapshot = fetchLiveSnapshot(afterSnapshots).get(0);
    assertThat(liveSnapshot.partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(liveSnapshot.maxOffset).isEqualTo(9);
    assertThat(liveSnapshot.snapshotPath).isEqualTo(SearchMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(liveSnapshot.snapshotId).startsWith(SearchMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(liveSnapshot.startTimeUtc).isEqualTo(startTime.toEpochMilli());
    assertThat(liveSnapshot.endTimeUtc).isEqualTo(startTime.plusSeconds(8).toEpochMilli());
    SnapshotMetadata nonLiveSnapshot = fetchNonLiveSnapshot(afterSnapshots).get(0);
    assertThat(nonLiveSnapshot.partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(nonLiveSnapshot.maxOffset).isEqualTo(9);
    assertThat(
            nonLiveSnapshot.snapshotPath.startsWith("s3")
                && nonLiveSnapshot.snapshotPath.contains(nonLiveSnapshot.name))
        .isTrue();
    assertThat(nonLiveSnapshot.snapshotId).isEqualTo(nonLiveSnapshot.name);
    assertThat(nonLiveSnapshot.startTimeUtc).isEqualTo(startTime.toEpochMilli());
    assertThat(nonLiveSnapshot.endTimeUtc).isEqualTo(startTime.plusSeconds(8).toEpochMilli());

    List<SearchMetadata> afterSearchNodes = fetchSearchNodes(chunkManager);
    assertThat(afterSearchNodes.size()).isEqualTo(1);
    assertThat(afterSearchNodes.get(0).url).contains(TEST_HOST);
    assertThat(afterSearchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
    assertThat(afterSearchNodes.get(0).snapshotName).contains(SearchMetadata.LIVE_SNAPSHOT_PATH);

    assertThat(chunk.isReadOnly()).isTrue();
    assertThat(chunk.info().getChunkSnapshotTimeEpochMs()).isNotZero();

    // Set the chunk snapshot time to a known value.
    final Instant snapshotTime = Instant.now().minusSeconds(60 * 60);
    chunk.info().setChunkSnapshotTimeEpochMs(snapshotTime.toEpochMilli());

    // Running an hour before snapshot won't delete it.
    final Instant snapshotTimeMinus1h = snapshotTime.minusSeconds(60 * 60);
    assertThat(chunkCleanerService.deleteStaleData(snapshotTimeMinus1h)).isZero();

    // Running at snapshot time won't delete it since the delay pushes it by 100 secs,.
    assertThat(chunkCleanerService.deleteStaleData(snapshotTime)).isZero();

    // Running at snapshot time won't delete it since the delay pushes it by 100 secs,.
    final Instant oneSecondBeforeSnapshotDelay = snapshotTime.plusSeconds(99);
    assertThat(chunkCleanerService.deleteStaleData(oneSecondBeforeSnapshotDelay)).isZero();

    // Delete the chunk once we hit the time threshold.
    assertThat(chunkCleanerService.deleteStaleData(snapshotTime.plusSeconds(100))).isEqualTo(1);
    assertThat(chunkManager.getChunkList().size()).isZero();

    // Check metadata after chunk is deleted.
    List<SnapshotMetadata> chunkDeletedSnapshots = fetchSnapshots(chunkManager);
    assertThat(chunkDeletedSnapshots.size()).isEqualTo(1);
    SnapshotMetadata nonLiveSnapshotAfterChunkDelete =
        fetchNonLiveSnapshot(chunkDeletedSnapshots).get(0);
    assertThat(nonLiveSnapshotAfterChunkDelete.partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(nonLiveSnapshotAfterChunkDelete.maxOffset).isEqualTo(9);
    assertThat(
            nonLiveSnapshotAfterChunkDelete.snapshotPath.startsWith("s3")
                && nonLiveSnapshotAfterChunkDelete.snapshotPath.contains(
                    nonLiveSnapshotAfterChunkDelete.name))
        .isTrue();
    assertThat(nonLiveSnapshotAfterChunkDelete.snapshotId)
        .isEqualTo(nonLiveSnapshotAfterChunkDelete.name);
    assertThat(nonLiveSnapshotAfterChunkDelete.startTimeUtc).isEqualTo(startTime.toEpochMilli());
    assertThat(nonLiveSnapshotAfterChunkDelete.endTimeUtc)
        .isEqualTo(startTime.plusSeconds(8).toEpochMilli());
    assertThat(fetchSearchNodes(chunkManager)).isEmpty();
  }

  private static void testBasicSnapshotMetadata(
      IndexingChunkManager<LogMessage> chunkManager, Instant creationTime)
      throws InterruptedException, ExecutionException, TimeoutException {
    final List<SnapshotMetadata> snapshotNodes = fetchSnapshots(chunkManager);
    assertThat(snapshotNodes.size()).isEqualTo(1);
    assertThat(snapshotNodes.get(0).snapshotPath).startsWith(SearchMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(snapshotNodes.get(0).maxOffset).isEqualTo(0);
    assertThat(snapshotNodes.get(0).partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(snapshotNodes.get(0).snapshotId).startsWith(SearchMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(snapshotNodes.get(0).startTimeUtc)
        .isCloseTo(creationTime.toEpochMilli(), Offset.offset(1000L));
    assertThat(snapshotNodes.get(0).endTimeUtc).isEqualTo(MAX_FUTURE_TIME);
    final List<SearchMetadata> searchNodes = fetchSearchNodes(chunkManager);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.get(0).url).contains(TEST_HOST);
    assertThat(searchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
    assertThat(searchNodes.get(0).snapshotName).contains(SearchMetadata.LIVE_SNAPSHOT_PATH);
  }

  @Test
  public void testDeleteStateDataOn2Chunks()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    Instant creationTime = Instant.now();
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, Duration.ofSeconds(100));
    assertThat(chunkManager.getChunkList().isEmpty()).isTrue();
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 11, 1000, startTime);

    int offset = 1;
    for (LogMessage m : messages.subList(0, 9)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(9);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    testBasicSnapshotMetadata(chunkManager, creationTime);

    final ReadWriteChunkImpl<LogMessage> chunk1 = chunkManager.getActiveChunk();
    assertThat(chunk1.isReadOnly()).isFalse();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isZero();

    for (LogMessage m : messages.subList(9, 11)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);

    checkMetadata(chunkManager, 3, 2, 1, 2, 1);

    final ReadWriteChunkImpl<LogMessage> chunk2 = chunkManager.getActiveChunk();
    assertThat(chunk1.isReadOnly()).isTrue();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isNotZero();
    assertThat(chunk2.isReadOnly()).isFalse();
    assertThat(chunk2.info().getChunkSnapshotTimeEpochMs()).isZero();
    // Commit the chunk1 and roll it over.
    chunkManager.rollOverActiveChunk();
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 2);

    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);

    checkMetadata(chunkManager, 4, 2, 2, 2, 0);
    assertThat(
            fetchSnapshots(chunkManager)
                .stream()
                .map(s -> s.maxOffset)
                .sorted()
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(List.of(10L, 10L, 11L, 11L));

    assertThat(chunk1.isReadOnly()).isTrue();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isNotZero();
    assertThat(chunk2.isReadOnly()).isTrue();
    assertThat(chunk2.info().getChunkSnapshotTimeEpochMs()).isNotZero();

    // Set the chunk1 and chunk2 snapshot time to a known value.
    final Instant chunk1SnapshotTime = Instant.now().minusSeconds(60 * 60);
    final Instant chunk2SnapshotTime = Instant.now().minusSeconds(60 * 60 * 2);
    chunk1.info().setChunkSnapshotTimeEpochMs(chunk1SnapshotTime.toEpochMilli());
    chunk2.info().setChunkSnapshotTimeEpochMs(chunk2SnapshotTime.toEpochMilli());

    // Running an hour before snapshot won't delete it.
    assertThat(chunkCleanerService.deleteStaleData(chunk2SnapshotTime.minusSeconds(60 * 60)))
        .isZero();
    assertThat(chunkCleanerService.deleteStaleData(chunk2SnapshotTime)).isZero();

    // Running at snapshot time won't delete it since the delay pushes it by 100 secs,.
    assertThat(chunkCleanerService.deleteStaleData(chunk2SnapshotTime.plusSeconds(99))).isZero();

    // Delete the chunk2 once we hit the time threshold.
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(chunkCleanerService.deleteStaleData(chunk2SnapshotTime.plusSeconds(101)))
        .isEqualTo(1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(chunkManager.getChunkList().contains(chunk1)).isTrue();
    assertThat(chunkManager.getChunkList().contains(chunk2)).isFalse();

    checkMetadata(chunkManager, 3, 1, 2, 1, 0);
    assertThat(
            fetchSnapshots(chunkManager)
                .stream()
                .map(s -> s.maxOffset)
                .sorted()
                .collect(Collectors.toList()))
        .containsOnly(10L, 11L, 11L);

    // Delete chunk1.
    assertThat(chunkCleanerService.deleteStaleData(chunk2SnapshotTime.plusSeconds(3600))).isZero();
    assertThat(chunkCleanerService.deleteStaleData(chunk1SnapshotTime.plusSeconds(99))).isZero();
    assertThat(chunkCleanerService.deleteStaleData(chunk1SnapshotTime.plusSeconds(100)))
        .isEqualTo(1);
    assertThat(chunkManager.getChunkList().size()).isZero();

    checkMetadata(chunkManager, 2, 0, 2, 0, 0);
    assertThat(
            fetchSnapshots(chunkManager)
                .stream()
                .map(s -> s.maxOffset)
                .sorted()
                .collect(Collectors.toList()))
        .containsOnly(10L, 11L);
  }

  private void checkMetadata(
      IndexingChunkManager<LogMessage> chunkManager,
      int expectedSnapshotSize,
      int expectedLiveSnapshotSize,
      int expectedNonLiveSnapshotSize,
      int expectedSearchNodeSize,
      int expectedInfinitySnapshotSize)
      throws InterruptedException, ExecutionException, TimeoutException {
    List<SnapshotMetadata> snapshots = fetchSnapshots(chunkManager);
    assertThat(snapshots.size()).isEqualTo(expectedSnapshotSize);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isEqualTo(expectedLiveSnapshotSize);
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(expectedNonLiveSnapshotSize);
    List<SearchMetadata> searchNodes = fetchSearchNodes(chunkManager);
    assertThat(searchNodes.size()).isEqualTo(expectedSearchNodeSize);
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            searchNodes.stream().map(s -> s.snapshotName).collect(Collectors.toList()));
    assertThat(snapshots.stream().filter(s -> s.endTimeUtc == MAX_FUTURE_TIME).count())
        .isEqualTo(expectedInfinitySnapshotSize);
  }

  @Test
  public void testDeleteStaleDataOnMultipleChunks()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    final long startTimeSecs = 1580515200; // Sat, 01 Feb 2020 00:00:00 UTC
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, Duration.ofSeconds(100));
    assertThat(chunkManager.getChunkList().isEmpty()).isTrue();
    assertThat(chunkCleanerService.deleteStaleChunksPastCutOff(startTimeSecs)).isZero();
    assertThat(chunkCleanerService.deleteStaleChunksPastCutOff(startTimeSecs + 1)).isZero();
    assertThat(chunkCleanerService.deleteStaleChunksPastCutOff(startTimeSecs + 3600 * 2)).isZero();
    assertThat(chunkCleanerService.deleteStaleChunksPastCutOff(startTimeSecs + 3600 * 3)).isZero();

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);
    messages.addAll(
        MessageUtil.makeMessagesWithTimeDifference(
            11, 20, 1000, startTime.plus(2, ChronoUnit.HOURS)));
    messages.addAll(
        MessageUtil.makeMessagesWithTimeDifference(
            21, 30, 1000, startTime.plus(4, ChronoUnit.HOURS)));
    messages.addAll(
        MessageUtil.makeMessagesWithTimeDifference(
            31, 35, 1000, startTime.plus(6, ChronoUnit.HOURS)));

    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    // Main chunk is already committed. Commit the new chunk so we can search it.
    chunkManager.rollOverActiveChunk();
    assertThat(chunkManager.getChunkList().size()).isEqualTo(4);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(35);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(4);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(4);

    for (Chunk<LogMessage> c : chunkManager.getChunkList()) {
      assertThat(((ReadWriteChunkImpl<LogMessage>) c).isReadOnly()).isTrue();
      assertThat(c.info().getChunkSnapshotTimeEpochMs()).isNotZero();
    }

    checkMetadata(chunkManager, 8, 4, 4, 4, 0);

    final Instant snapshotTime = Instant.now();
    // Modify snapshot time on chunks
    int i = 0;
    for (Chunk<LogMessage> chunk : chunkManager.getChunkList()) {
      final long chunkSnapshotTimeEpochMs = snapshotTime.minusSeconds(3600L * i).toEpochMilli();
      chunk.info().setChunkSnapshotTimeEpochMs(chunkSnapshotTimeEpochMs);
      i++;
    }

    assertThat(chunkCleanerService.deleteStaleData(snapshotTime.minusSeconds(3600 * 4))).isZero();
    assertThat(chunkCleanerService.deleteStaleData(snapshotTime.minusSeconds(3600 * 3))).isZero();
    assertThat(
            chunkCleanerService.deleteStaleData(
                snapshotTime.minusSeconds(3600 * 3).plusSeconds(100)))
        .isEqualTo(1);

    checkMetadata(chunkManager, 7, 3, 4, 3, 0);

    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);
    assertThat(
            chunkCleanerService.deleteStaleData(snapshotTime.minusSeconds(3600).plusSeconds(100)))
        .isEqualTo(2);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);

    checkMetadata(chunkManager, 5, 1, 4, 1, 0);

    assertThat(chunkCleanerService.deleteStaleData(snapshotTime.plusSeconds(100))).isEqualTo(1);
    assertThat(chunkManager.getChunkList().size()).isZero();

    checkMetadata(chunkManager, 4, 0, 4, 0, 0);
  }
}
