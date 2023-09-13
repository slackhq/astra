package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.chunk.ChunkInfo.MAX_FUTURE_TIME;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.TEST_HOST;
import static com.slack.kaldb.testlib.ChunkManagerUtil.TEST_PORT;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchLiveSnapshot;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchNonLiveSnapshot;
import static com.slack.kaldb.testlib.ChunkManagerUtil.makeChunkManagerUtil;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.awaitility.Awaitility.await;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.kaldb.chunk.Chunk;
import com.slack.kaldb.chunk.ChunkInfo;
import com.slack.kaldb.chunk.ReadWriteChunk;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.core.KaldbMetadataTestUtils;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
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
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ChunkCleanerServiceTest {
  private static final String S3_TEST_BUCKET = "test-kaldb-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private static final String TEST_KAFKA_PARTITION_ID = "10";
  private SimpleMeterRegistry metricsRegistry;
  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;

  @BeforeEach
  public void setUp() throws Exception {
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        makeChunkManagerUtil(
            S3_MOCK_EXTENSION,
            S3_TEST_BUCKET,
            metricsRegistry,
            10 * 1024 * 1024 * 1024L,
            10L,
            KaldbConfigUtil.makeIndexerConfig());
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
    searchMetadataStore = new SearchMetadataStore(chunkManagerUtil.getCuratorFramework(), false);
    snapshotMetadataStore = new SnapshotMetadataStore(chunkManagerUtil.getCuratorFramework());
  }

  @AfterEach
  public void tearDown() throws IOException, TimeoutException {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
    metricsRegistry.close();
  }

  @Test
  public void testDeleteStaleDataThrowsErrorWhenGivenDurationInFuture() throws IOException {
    Instant creationTime = Instant.now();
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, 1, Duration.ofSeconds(999_999_999));
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
    testBasicSnapshotMetadata(creationTime);

    final ReadWriteChunk<LogMessage> chunk1 = chunkManager.getActiveChunk();
    assertThat(chunk1.isReadOnly()).isFalse();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isZero();
    try {
      final Instant theBeginning =
          LocalDateTime.of(1, 1, 1, 1, 1, 1).atZone(ZoneOffset.UTC).toInstant();
      chunkCleanerService.deleteStaleData(theBeginning);
      fail(
          "Expected IllegalArgumentException to be thrown if the time is far enough in the past that we underflow");
    } catch (IllegalArgumentException e) {
      assertThat(true).isTrue();
    }
  }

  @Test
  public void testDeleteStaleDataThrowsErrorWhenGivenLimitLessThan0() throws IOException {
    Instant creationTime = Instant.now();
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, -1, Duration.ofSeconds(100_000_000));
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
    testBasicSnapshotMetadata(creationTime);

    final ReadWriteChunk<LogMessage> chunk1 = chunkManager.getActiveChunk();
    assertThat(chunk1.isReadOnly()).isFalse();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isZero();
    try {
      chunkCleanerService.deleteStaleData();
      fail("Expected IllegalArgumentException to be thrown if the limit is negative");
    } catch (IllegalArgumentException e) {
      assertThat(true).isTrue();
    }
  }

  @Test
  public void testDeleteOverMaxThresholdGreaterThanZero() throws IOException {
    Instant creationTime = Instant.now();
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, 1, Duration.ofSeconds(100_000_000));
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
    testBasicSnapshotMetadata(creationTime);

    final ReadWriteChunk<LogMessage> chunk1 = chunkManager.getActiveChunk();
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

    checkMetadata(3, 2, 1, 2, 1);

    final ReadWriteChunk<LogMessage> chunk2 = chunkManager.getActiveChunk();
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

    checkMetadata(4, 2, 2, 2, 0);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).stream()
                .map(s -> s.maxOffset)
                .sorted()
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(List.of(10L, 10L, 11L, 11L));

    assertThat(chunk1.isReadOnly()).isTrue();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isNotZero();
    assertThat(chunk2.isReadOnly()).isTrue();
    assertThat(chunk2.info().getChunkSnapshotTimeEpochMs()).isNotZero();

    // Confirm that we deleted chunk1 instead of chunk2, as chunk1 is the older chunk
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(chunkCleanerService.deleteStaleData()).isEqualTo(1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(chunkManager.getChunkList().contains(chunk1)).isFalse();
    assertThat(chunkManager.getChunkList().contains(chunk2)).isTrue();

    checkMetadata(3, 1, 2, 1, 0);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).stream()
                .map(s -> s.maxOffset)
                .sorted()
                .collect(Collectors.toList()))
        .containsOnly(10L, 11L, 11L);
  }

  @Test
  public void testDeleteOverZeroMaxThreshold() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    final Instant creationTime = Instant.now();
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, 0, Duration.ofSeconds(100_000_000));
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

    ReadWriteChunk<LogMessage> chunk = chunkManager.getActiveChunk();
    assertThat(chunk.isReadOnly()).isFalse();
    assertThat(chunk.info().getChunkSnapshotTimeEpochMs()).isZero();

    testBasicSnapshotMetadata(creationTime);

    // try to delete active chunk
    assertThat(chunkCleanerService.deleteStaleData()).isEqualTo(0);

    // Commit the chunk and roll it over.
    chunkManager.rollOverActiveChunk();
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(9);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);

    List<SnapshotMetadata> afterSnapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(afterSnapshots.size()).isEqualTo(2);
    assertThat(afterSnapshots).contains(ChunkInfo.toSnapshotMetadata(chunk.info(), ""));
    SnapshotMetadata liveSnapshot = fetchLiveSnapshot(afterSnapshots).get(0);
    assertThat(liveSnapshot.partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(liveSnapshot.maxOffset).isEqualTo(9);
    assertThat(liveSnapshot.snapshotPath).isEqualTo(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(liveSnapshot.snapshotId).startsWith(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(liveSnapshot.startTimeEpochMs).isEqualTo(startTime.toEpochMilli());
    assertThat(liveSnapshot.endTimeEpochMs).isEqualTo(startTime.plusSeconds(8).toEpochMilli());
    SnapshotMetadata nonLiveSnapshot = fetchNonLiveSnapshot(afterSnapshots).get(0);
    assertThat(nonLiveSnapshot.partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(nonLiveSnapshot.maxOffset).isEqualTo(9);
    assertThat(
            nonLiveSnapshot.snapshotPath.startsWith("s3")
                && nonLiveSnapshot.snapshotPath.contains(nonLiveSnapshot.name))
        .isTrue();
    assertThat(nonLiveSnapshot.snapshotId).isEqualTo(nonLiveSnapshot.name);
    assertThat(nonLiveSnapshot.startTimeEpochMs).isEqualTo(startTime.toEpochMilli());
    assertThat(nonLiveSnapshot.endTimeEpochMs).isEqualTo(startTime.plusSeconds(8).toEpochMilli());

    List<SearchMetadata> afterSearchNodes =
        KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(afterSearchNodes.size()).isEqualTo(1);
    assertThat(afterSearchNodes.get(0).url).contains(TEST_HOST);
    assertThat(afterSearchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
    assertThat(afterSearchNodes.get(0).snapshotName).contains(SnapshotMetadata.LIVE_SNAPSHOT_PATH);

    assertThat(chunk.isReadOnly()).isTrue();
    assertThat(chunk.info().getChunkSnapshotTimeEpochMs()).isNotZero();

    // Delete the chunk once we hit the time threshold.
    assertThat(chunkCleanerService.deleteStaleData()).isEqualTo(1);
    assertThat(chunkManager.getChunkList().size()).isZero();

    // Check metadata after chunk is deleted.
    List<SnapshotMetadata> chunkDeletedSnapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
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
    assertThat(nonLiveSnapshotAfterChunkDelete.startTimeEpochMs)
        .isEqualTo(startTime.toEpochMilli());
    assertThat(nonLiveSnapshotAfterChunkDelete.endTimeEpochMs)
        .isEqualTo(startTime.plusSeconds(8).toEpochMilli());
    assertThat(KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore)).isEmpty();
  }

  @Test
  public void testDeleteStaleDataOn1Chunk() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    final Instant creationTime = Instant.now();
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, 20, Duration.ofSeconds(100));
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

    ReadWriteChunk<LogMessage> chunk = chunkManager.getActiveChunk();
    assertThat(chunk.isReadOnly()).isFalse();
    assertThat(chunk.info().getChunkSnapshotTimeEpochMs()).isZero();

    testBasicSnapshotMetadata(creationTime);

    // try to delete active chunk
    assertThat(chunkCleanerService.deleteStaleData(Instant.now().plusSeconds(100))).isEqualTo(0);

    // Commit the chunk and roll it over.
    chunkManager.rollOverActiveChunk();
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(9);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);

    List<SnapshotMetadata> afterSnapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(afterSnapshots.size()).isEqualTo(2);
    assertThat(afterSnapshots).contains(ChunkInfo.toSnapshotMetadata(chunk.info(), ""));
    SnapshotMetadata liveSnapshot = fetchLiveSnapshot(afterSnapshots).get(0);
    assertThat(liveSnapshot.partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(liveSnapshot.maxOffset).isEqualTo(9);
    assertThat(liveSnapshot.snapshotPath).isEqualTo(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(liveSnapshot.snapshotId).startsWith(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(liveSnapshot.startTimeEpochMs).isEqualTo(startTime.toEpochMilli());
    assertThat(liveSnapshot.endTimeEpochMs).isEqualTo(startTime.plusSeconds(8).toEpochMilli());
    SnapshotMetadata nonLiveSnapshot = fetchNonLiveSnapshot(afterSnapshots).get(0);
    assertThat(nonLiveSnapshot.partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(nonLiveSnapshot.maxOffset).isEqualTo(9);
    assertThat(
            nonLiveSnapshot.snapshotPath.startsWith("s3")
                && nonLiveSnapshot.snapshotPath.contains(nonLiveSnapshot.name))
        .isTrue();
    assertThat(nonLiveSnapshot.snapshotId).isEqualTo(nonLiveSnapshot.name);
    assertThat(nonLiveSnapshot.startTimeEpochMs).isEqualTo(startTime.toEpochMilli());
    assertThat(nonLiveSnapshot.endTimeEpochMs).isEqualTo(startTime.plusSeconds(8).toEpochMilli());

    List<SearchMetadata> afterSearchNodes =
        KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(afterSearchNodes.size()).isEqualTo(1);
    assertThat(afterSearchNodes.get(0).url).contains(TEST_HOST);
    assertThat(afterSearchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
    assertThat(afterSearchNodes.get(0).snapshotName).contains(SnapshotMetadata.LIVE_SNAPSHOT_PATH);

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
    List<SnapshotMetadata> chunkDeletedSnapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
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
    assertThat(nonLiveSnapshotAfterChunkDelete.startTimeEpochMs)
        .isEqualTo(startTime.toEpochMilli());
    assertThat(nonLiveSnapshotAfterChunkDelete.endTimeEpochMs)
        .isEqualTo(startTime.plusSeconds(8).toEpochMilli());
    assertThat(KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore)).isEmpty();
  }

  private void testBasicSnapshotMetadata(Instant creationTime) {
    final List<SnapshotMetadata> snapshotNodes =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshotNodes.size()).isEqualTo(1);
    assertThat(snapshotNodes.get(0).snapshotPath).startsWith(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(snapshotNodes.get(0).maxOffset).isEqualTo(0);
    assertThat(snapshotNodes.get(0).partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(snapshotNodes.get(0).snapshotId).startsWith(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(snapshotNodes.get(0).startTimeEpochMs)
        .isCloseTo(creationTime.toEpochMilli(), Offset.offset(3000L));
    assertThat(snapshotNodes.get(0).endTimeEpochMs).isEqualTo(MAX_FUTURE_TIME);
    final List<SearchMetadata> searchNodes =
        KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.get(0).url).contains(TEST_HOST);
    assertThat(searchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
    assertThat(searchNodes.get(0).snapshotName).contains(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
  }

  @Test
  public void testDeleteStateDataOn2Chunks() throws IOException {
    Instant creationTime = Instant.now();
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, 20, Duration.ofSeconds(100));
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
    testBasicSnapshotMetadata(creationTime);

    final ReadWriteChunk<LogMessage> chunk1 = chunkManager.getActiveChunk();
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

    checkMetadata(3, 2, 1, 2, 1);

    final ReadWriteChunk<LogMessage> chunk2 = chunkManager.getActiveChunk();
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

    checkMetadata(4, 2, 2, 2, 0);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).stream()
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

    checkMetadata(3, 1, 2, 1, 0);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).stream()
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

    checkMetadata(2, 0, 2, 0, 0);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).stream()
                .map(s -> s.maxOffset)
                .sorted()
                .collect(Collectors.toList()))
        .containsOnly(10L, 11L);
  }

  private void checkMetadata(
      int expectedSnapshotSize,
      int expectedLiveSnapshotSize,
      int expectedNonLiveSnapshotSize,
      int expectedSearchNodeSize,
      int expectedInfinitySnapshotSize) {
    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(expectedSnapshotSize);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isEqualTo(expectedLiveSnapshotSize);
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(expectedNonLiveSnapshotSize);
    List<SearchMetadata> searchNodes = KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(searchNodes.size()).isEqualTo(expectedSearchNodeSize);
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            searchNodes.stream().map(s -> s.snapshotName).collect(Collectors.toList()));
    assertThat(snapshots.stream().filter(s -> s.endTimeEpochMs == MAX_FUTURE_TIME).count())
        .isEqualTo(expectedInfinitySnapshotSize);
  }

  @Test
  public void testDeleteStaleDataOnMultipleChunks() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, 20, Duration.ofSeconds(100));
    assertThat(chunkManager.getChunkList().isEmpty()).isTrue();
    assertThat(chunkCleanerService.deleteStaleData(startTime)).isZero();
    assertThat(chunkCleanerService.deleteStaleData(startTime.plusSeconds(1))).isZero();
    assertThat(chunkCleanerService.deleteStaleData(startTime.plusSeconds(3600 * 2))).isZero();
    assertThat(chunkCleanerService.deleteStaleData(startTime.plusSeconds(3600 * 3))).isZero();

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
      assertThat(((ReadWriteChunk<LogMessage>) c).isReadOnly()).isTrue();
      assertThat(c.info().getChunkSnapshotTimeEpochMs()).isNotZero();
    }

    checkMetadata(8, 4, 4, 4, 0);

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

    checkMetadata(7, 3, 4, 3, 0);

    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);
    assertThat(
            chunkCleanerService.deleteStaleData(snapshotTime.minusSeconds(3600).plusSeconds(100)))
        .isEqualTo(2);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);

    checkMetadata(5, 1, 4, 1, 0);

    assertThat(chunkCleanerService.deleteStaleData(snapshotTime.plusSeconds(100))).isEqualTo(1);
    assertThat(chunkManager.getChunkList().size()).isZero();

    checkMetadata(4, 0, 4, 0, 0);
  }
}
