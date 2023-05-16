package com.slack.kaldb.chunk;

import static com.slack.kaldb.chunk.ChunkInfo.DEFAULT_MAX_OFFSET;
import static com.slack.kaldb.chunk.ChunkInfo.MAX_FUTURE_TIME;
import static com.slack.kaldb.chunk.ChunkInfo.containsDataInTimeRange;
import static com.slack.kaldb.chunk.ChunkInfo.fromSnapshotMetadata;
import static com.slack.kaldb.chunk.ChunkInfo.toSnapshotMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

public class ChunkInfoTest {
  private static final String TEST_KAFKA_PARTITION_ID = "10";
  private static final String TEST_CHUNK_NAME = "testChunkInfo1";
  private static final String TEST_SNAPSHOT_PATH = "testSnapshotPath";

  @Test
  public void testChunkInfoCreation() {
    final long chunkCreationTime = 1000;
    final ChunkInfo info =
        new ChunkInfo(TEST_CHUNK_NAME, 1000, TEST_KAFKA_PARTITION_ID, TEST_SNAPSHOT_PATH);
    assertThat(info.getChunkCreationTimeEpochMs()).isEqualTo(chunkCreationTime);
    assertThat(info.getChunkLastUpdatedTimeEpochMs()).isEqualTo(chunkCreationTime);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(chunkCreationTime);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(MAX_FUTURE_TIME);
    assertThat(info.getChunkSnapshotTimeEpochMs()).isEqualTo(0);
    assertThat(info.getMaxOffset()).isEqualTo(DEFAULT_MAX_OFFSET);
    assertThat(info.getKafkaPartitionId()).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(info.getSnapshotPath()).isEqualTo(TEST_SNAPSHOT_PATH);
  }

  @Test
  public void testChunkDataTimeRange() {
    final LocalDateTime startTime = LocalDateTime.of(2020, 10, 1, 10, 10, 0);
    final long chunkCreationTimeEpochMilli = startTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    final ChunkInfo info =
        new ChunkInfo(
            TEST_CHUNK_NAME,
            chunkCreationTimeEpochMilli,
            TEST_KAFKA_PARTITION_ID,
            TEST_SNAPSHOT_PATH);
    assertThat(info.getChunkCreationTimeEpochMs()).isEqualTo(chunkCreationTimeEpochMilli);
    assertThat(info.getChunkLastUpdatedTimeEpochMs()).isEqualTo(chunkCreationTimeEpochMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(chunkCreationTimeEpochMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(MAX_FUTURE_TIME);
    assertThat(info.getChunkSnapshotTimeEpochMs()).isEqualTo(0);
    assertThat(info.getMaxOffset()).isEqualTo(DEFAULT_MAX_OFFSET);
    assertThat(info.getKafkaPartitionId()).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(info.getSnapshotPath()).isEqualTo(TEST_SNAPSHOT_PATH);

    // Add message with same time range.
    info.updateDataTimeRange(chunkCreationTimeEpochMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(chunkCreationTimeEpochMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(chunkCreationTimeEpochMilli);

    // Add a message from before time range
    final long startTimeMinus1MinMilli =
        startTime.minusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli();
    info.updateDataTimeRange(startTimeMinus1MinMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(startTimeMinus1MinMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(chunkCreationTimeEpochMilli);

    final long startTimeMinus2MinMilli =
        startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli();
    info.updateDataTimeRange(startTimeMinus2MinMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(startTimeMinus2MinMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(chunkCreationTimeEpochMilli);

    // Add same timestamp as min again
    info.updateDataTimeRange(startTimeMinus2MinMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(startTimeMinus2MinMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(chunkCreationTimeEpochMilli);

    // Add a message within time range.
    info.updateDataTimeRange(startTimeMinus1MinMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(startTimeMinus2MinMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(chunkCreationTimeEpochMilli);

    // Add message at end of time range.
    info.updateDataTimeRange(chunkCreationTimeEpochMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(startTimeMinus2MinMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(chunkCreationTimeEpochMilli);

    // Add a message after the time range.
    final long startTimePlus1MinMilli =
        startTime.plusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli();
    info.updateDataTimeRange(startTimePlus1MinMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(startTimeMinus2MinMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(startTimePlus1MinMilli);

    final long startTimePlus2MinMilli =
        startTime.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli();
    info.updateDataTimeRange(startTimePlus2MinMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(startTimeMinus2MinMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(startTimePlus2MinMilli);

    // Add message at end of time range.
    info.updateDataTimeRange(startTimePlus1MinMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(startTimeMinus2MinMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(startTimePlus2MinMilli);

    // Add message in the time range.
    info.updateDataTimeRange(startTimeMinus1MinMilli);
    info.updateDataTimeRange(startTimePlus1MinMilli);
    info.updateDataTimeRange(chunkCreationTimeEpochMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(startTimeMinus2MinMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(startTimePlus2MinMilli);
  }

  @Test
  public void testUnInitializedChunkDataInRange() {
    final LocalDateTime startTime = LocalDateTime.of(2020, 10, 1, 10, 10, 0);
    final long chunkCreationTimeSecs = startTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    final ChunkInfo info =
        new ChunkInfo(
            TEST_CHUNK_NAME, chunkCreationTimeSecs, TEST_KAFKA_PARTITION_ID, TEST_SNAPSHOT_PATH);
    assertThat(info.getChunkCreationTimeEpochMs()).isEqualTo(chunkCreationTimeSecs);
    assertThat(info.getChunkLastUpdatedTimeEpochMs()).isEqualTo(chunkCreationTimeSecs);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(chunkCreationTimeSecs);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(MAX_FUTURE_TIME);
    assertThat(info.getChunkSnapshotTimeEpochMs()).isEqualTo(0);
    assertThat(info.containsDataInTimeRange(1000, 1001)).isFalse();
    assertThat(info.containsDataInTimeRange(chunkCreationTimeSecs, MAX_FUTURE_TIME)).isTrue();
    assertThat(info.containsDataInTimeRange(chunkCreationTimeSecs, MAX_FUTURE_TIME - 1)).isTrue();
    assertThat(info.containsDataInTimeRange(chunkCreationTimeSecs + 1, MAX_FUTURE_TIME - 1))
        .isTrue();
    assertThat(info.containsDataInTimeRange(chunkCreationTimeSecs - 1, MAX_FUTURE_TIME - 1))
        .isTrue();
    assertThat(info.containsDataInTimeRange(1000, chunkCreationTimeSecs - 1)).isFalse();
    assertThat(info.containsDataInTimeRange(1000, chunkCreationTimeSecs + 1)).isTrue();
    assertThat(info.getMaxOffset()).isEqualTo(DEFAULT_MAX_OFFSET);
    assertThat(info.getKafkaPartitionId()).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(info.getSnapshotPath()).isEqualTo(TEST_SNAPSHOT_PATH);
  }

  @Test
  public void testChunkDataInRange() {
    final LocalDateTime startTime = LocalDateTime.of(2020, 10, 1, 10, 10, 0);
    final long chunkCreationTimeMs = startTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    final ChunkInfo info =
        new ChunkInfo(
            TEST_CHUNK_NAME, chunkCreationTimeMs, TEST_KAFKA_PARTITION_ID, TEST_SNAPSHOT_PATH);
    assertThat(info.getChunkCreationTimeEpochMs()).isEqualTo(chunkCreationTimeMs);
    assertThat(info.getChunkLastUpdatedTimeEpochMs()).isEqualTo(chunkCreationTimeMs);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(chunkCreationTimeMs);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(MAX_FUTURE_TIME);
    assertThat(info.getChunkSnapshotTimeEpochMs()).isEqualTo(0);

    // Expand the time range for chunk info.
    final long startTimePlus2MinMilli =
        startTime.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli();
    final long startTimeMinus2MinMilli =
        startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli();
    info.updateDataTimeRange(startTimeMinus2MinMilli);
    info.updateDataTimeRange(startTimePlus2MinMilli);
    assertThat(info.getDataStartTimeEpochMs()).isEqualTo(startTimeMinus2MinMilli);
    assertThat(info.getDataEndTimeEpochMs()).isEqualTo(startTimePlus2MinMilli);

    assertThat(info.containsDataInTimeRange(1, 10)).isFalse();
    // sanity check if the extended method also works. No point repeating all the tests
    assertThat(
            containsDataInTimeRange(
                info.getDataStartTimeEpochMs(), info.getDataEndTimeEpochMs(), 1, 10))
        .isFalse();

    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(5).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.minusMinutes(4).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isFalse();
    assertThat(
            info.containsDataInTimeRange(
                1, startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.minusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.plusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.plusMinutes(3).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.plusMinutes(3).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(3).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.plusMinutes(5).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusYears(3).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.plusYears(5).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();

    // O length interval
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.plusYears(5).toInstant(ZoneOffset.UTC).toEpochMilli(),
                startTime.plusYears(7).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isFalse();
    assertThat(info.containsDataInTimeRange(1, 1)).isFalse();

    // Start time is 0
    assertThat(info.containsDataInTimeRange(0, 0)).isFalse();
    assertThat(
            info.containsDataInTimeRange(
                0, startTime.minusMinutes(3).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isFalse();
    assertThat(
            info.containsDataInTimeRange(
                0, startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                0, startTime.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                0, startTime.plusMinutes(3).toInstant(ZoneOffset.UTC).toEpochMilli()))
        .isTrue();
  }

  @Test
  public void testNegativeStartTimeInDateRange() {
    final ChunkInfo info =
        new ChunkInfo(TEST_CHUNK_NAME, 1000, TEST_KAFKA_PARTITION_ID, TEST_SNAPSHOT_PATH);
    info.updateDataTimeRange(980);
    info.updateDataTimeRange(1020);

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> info.containsDataInTimeRange(-1, 980));
  }

  @Test
  public void testNegativeEndTimeInDateRange() {
    final ChunkInfo info =
        new ChunkInfo(TEST_CHUNK_NAME, 1000, TEST_KAFKA_PARTITION_ID, TEST_SNAPSHOT_PATH);
    info.updateDataTimeRange(980);
    info.updateDataTimeRange(1020);

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> info.containsDataInTimeRange(960, -1));
  }

  @Test
  public void testNegativeIntervalInDateRange() {
    final ChunkInfo info =
        new ChunkInfo(TEST_CHUNK_NAME, 1000, TEST_KAFKA_PARTITION_ID, TEST_SNAPSHOT_PATH);
    info.updateDataTimeRange(980);
    info.updateDataTimeRange(1020);

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> info.containsDataInTimeRange(960, 950));
  }

  @Test
  public void testInvalidChunkName() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new ChunkInfo(null, 100, TEST_KAFKA_PARTITION_ID, TEST_SNAPSHOT_PATH));
  }

  @Test
  public void testEmptyChunkName() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new ChunkInfo("", 100, TEST_KAFKA_PARTITION_ID, TEST_SNAPSHOT_PATH));
  }

  @Test
  public void testNegativeChunkCreationTime() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () -> new ChunkInfo(TEST_CHUNK_NAME, -1, TEST_KAFKA_PARTITION_ID, TEST_SNAPSHOT_PATH));
  }

  @Test
  public void testEmptyKafkaPartitionId() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new ChunkInfo(TEST_CHUNK_NAME, 100, "", TEST_SNAPSHOT_PATH));
  }

  @Test
  public void testMaxOffset() {
    ChunkInfo chunkInfo =
        new ChunkInfo(TEST_CHUNK_NAME, 100, TEST_KAFKA_PARTITION_ID, TEST_SNAPSHOT_PATH);
    assertThat(chunkInfo.getMaxOffset()).isEqualTo(DEFAULT_MAX_OFFSET);
    chunkInfo.updateMaxOffset(100);
    assertThat(chunkInfo.getMaxOffset()).isEqualTo(100);
    chunkInfo.updateMaxOffset(101);
    assertThat(chunkInfo.getMaxOffset()).isEqualTo(101);
    chunkInfo.updateMaxOffset(103);
    assertThat(chunkInfo.getMaxOffset()).isEqualTo(103);

    // Inserting a lower message offset doesn't decrement the offset.
    chunkInfo.updateMaxOffset(102);
    assertThat(chunkInfo.getMaxOffset()).isEqualTo(103);

    // A higher offset increments the counter.
    chunkInfo.updateMaxOffset(104);
    assertThat(chunkInfo.getMaxOffset()).isEqualTo(104);
  }

  @Test
  public void testSnapshotPathCanBeUpdated() {
    ChunkInfo chunkInfo =
        new ChunkInfo(TEST_CHUNK_NAME, 10000, TEST_KAFKA_PARTITION_ID, TEST_SNAPSHOT_PATH);
    assertThat(chunkInfo.getSnapshotPath()).isEqualTo(TEST_SNAPSHOT_PATH);
    String testPath = "/path";
    chunkInfo.setSnapshotPath(testPath);
    assertThat(chunkInfo.getSnapshotPath()).isEqualTo(testPath);
  }

  @Test
  public void snapshotMetadataConversion() {
    long dataStart = 101;
    long dataEnd = 102;
    ChunkInfo chunkInfo =
        new ChunkInfo(
            TEST_CHUNK_NAME,
            dataStart,
            dataEnd,
            dataStart,
            dataEnd,
            dataEnd,
            1000,
            TEST_KAFKA_PARTITION_ID,
            TEST_SNAPSHOT_PATH);
    assertThat(fromSnapshotMetadata(toSnapshotMetadata(chunkInfo, ""))).isEqualTo(chunkInfo);
  }
}
