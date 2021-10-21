package com.slack.kaldb.chunk;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.chunkManager.ChunkCleanerService;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
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
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class ChunkCleanerServiceTest {
  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
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
    KaldbConfig.reset();
    metricsRegistry.close();
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
  }

  @Test
  public void testDeleteStaleDataOn1Chunk() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, Duration.ofSeconds(100));
    assertThat(chunkManager.getChunkList().isEmpty()).isTrue();
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 9, 1000, startTime);

    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(9);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);

    ReadWriteChunkImpl<LogMessage> chunk = chunkManager.getActiveChunk();
    assertThat(chunk.isReadOnly()).isFalse();
    assertThat(chunk.info().getChunkSnapshotTimeEpochMs()).isZero();

    // Commit the chunk and roll it over.
    chunkManager.rollOverActiveChunk();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(9);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);

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
  }

  @Test
  public void testDeleteStateDataOn2Chunks() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, Duration.ofSeconds(100));
    assertThat(chunkManager.getChunkList().isEmpty()).isTrue();
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 11, 1000, startTime);

    for (LogMessage m : messages.subList(0, 9)) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(9);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);

    final ReadWriteChunkImpl<LogMessage> chunk1 = chunkManager.getActiveChunk();
    assertThat(chunk1.isReadOnly()).isFalse();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isZero();

    for (LogMessage m : messages.subList(9, 11)) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }

    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);

    final ReadWriteChunkImpl<LogMessage> chunk2 = chunkManager.getActiveChunk();
    assertThat(chunk1.isReadOnly()).isTrue();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isNotZero();
    assertThat(chunk2.isReadOnly()).isFalse();
    assertThat(chunk2.info().getChunkSnapshotTimeEpochMs()).isZero();
    // Commit the chunk1 and roll it over.
    chunkManager.rollOverActiveChunk();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);

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

    // Delete chunk1.
    assertThat(chunkCleanerService.deleteStaleData(chunk2SnapshotTime.plusSeconds(3600))).isZero();
    assertThat(chunkCleanerService.deleteStaleData(chunk1SnapshotTime.plusSeconds(99))).isZero();
    assertThat(chunkCleanerService.deleteStaleData(chunk1SnapshotTime.plusSeconds(100)))
        .isEqualTo(1);
    assertThat(chunkManager.getChunkList().size()).isZero();
  }

  @Test
  public void testDeleteStaleDataOnMultipleChunks() throws IOException {
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

    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
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
      assertThat(((ReadWriteChunkImpl) c).isReadOnly()).isTrue();
      assertThat(c.info().getChunkSnapshotTimeEpochMs()).isNotZero();
    }

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
    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);
    assertThat(
            chunkCleanerService.deleteStaleData(snapshotTime.minusSeconds(3600).plusSeconds(100)))
        .isEqualTo(2);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(chunkCleanerService.deleteStaleData(snapshotTime.plusSeconds(100))).isEqualTo(1);
    assertThat(chunkManager.getChunkList().size()).isZero();
  }
}
