package com.slack.kaldb.chunk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.Test;

public class ChunkInfoTest {

  private static final String testChunkName = "testChunkInfo1";

  @Test
  public void testChunkInfoCreation() {
    final long chunkCreationTime = 1000;
    final ChunkInfo info = new ChunkInfo(testChunkName, 1000);
    assertThat(info.getChunkCreationTimeEpochSecs()).isEqualTo(chunkCreationTime);
    assertThat(info.getChunkLastUpdatedTimeSecsEpochSecs()).isEqualTo(chunkCreationTime);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(0);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(0);
    assertThat(info.getChunkSnapshotTimeEpochSecs()).isEqualTo(0);
  }

  @Test
  public void testChunkDataTimeRange() {
    final LocalDateTime startTime = LocalDateTime.of(2020, 10, 1, 10, 10, 0);
    final long chunkCreationTimeEpochMilli = startTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    final long chunkCreationTimeSecs = startTime.toEpochSecond(ZoneOffset.UTC);
    final ChunkInfo info = new ChunkInfo(testChunkName, chunkCreationTimeSecs);
    assertThat(info.getChunkCreationTimeEpochSecs()).isEqualTo(chunkCreationTimeSecs);
    assertThat(info.getChunkLastUpdatedTimeSecsEpochSecs()).isEqualTo(chunkCreationTimeSecs);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(0);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(0);
    assertThat(info.getChunkSnapshotTimeEpochSecs()).isEqualTo(0);

    // Add message with same time range.
    info.updateDataTimeRange(chunkCreationTimeEpochMilli);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(chunkCreationTimeSecs);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(chunkCreationTimeSecs);

    // Add a message from before time range
    final long startTimeMinus1MinMilli =
        startTime.minusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli();
    info.updateDataTimeRange(startTimeMinus1MinMilli);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(startTimeMinus1MinMilli / 1000);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(chunkCreationTimeSecs);

    final long startTimeMinus2MinMilli =
        startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli();
    info.updateDataTimeRange(startTimeMinus2MinMilli);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(startTimeMinus2MinMilli / 1000);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(chunkCreationTimeSecs);

    // Add same timestamp as min again
    info.updateDataTimeRange(startTimeMinus2MinMilli);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(startTimeMinus2MinMilli / 1000);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(chunkCreationTimeSecs);

    // Add a message within time range.
    info.updateDataTimeRange(startTimeMinus1MinMilli);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(startTimeMinus2MinMilli / 1000);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(chunkCreationTimeSecs);

    // Add message at end of time range.
    info.updateDataTimeRange(chunkCreationTimeEpochMilli);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(startTimeMinus2MinMilli / 1000);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(chunkCreationTimeSecs);

    // Add a message after the time range.
    final long startTimePlus1MinMilli =
        startTime.plusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli();
    info.updateDataTimeRange(startTimePlus1MinMilli);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(startTimeMinus2MinMilli / 1000);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(startTimePlus1MinMilli / 1000);

    final long startTimePlus2MinMilli =
        startTime.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli();
    info.updateDataTimeRange(startTimePlus2MinMilli);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(startTimeMinus2MinMilli / 1000);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(startTimePlus2MinMilli / 1000);

    // Add message at end of time range.
    info.updateDataTimeRange(startTimePlus1MinMilli);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(startTimeMinus2MinMilli / 1000);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(startTimePlus2MinMilli / 1000);

    // Add message in the time range.
    info.updateDataTimeRange(startTimeMinus1MinMilli);
    info.updateDataTimeRange(startTimePlus1MinMilli);
    info.updateDataTimeRange(chunkCreationTimeEpochMilli);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(startTimeMinus2MinMilli / 1000);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(startTimePlus2MinMilli / 1000);
  }

  @Test(expected = IllegalStateException.class)
  public void testUnInitializedChunkDataInRange() {
    final LocalDateTime startTime = LocalDateTime.of(2020, 10, 1, 10, 10, 0);
    final long chunkCreationTimeSecs = startTime.toEpochSecond(ZoneOffset.UTC);
    final ChunkInfo info = new ChunkInfo(testChunkName, chunkCreationTimeSecs);
    assertThat(info.getChunkCreationTimeEpochSecs()).isEqualTo(chunkCreationTimeSecs);
    assertThat(info.getChunkLastUpdatedTimeSecsEpochSecs()).isEqualTo(chunkCreationTimeSecs);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(0);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(0);
    assertThat(info.getChunkSnapshotTimeEpochSecs()).isEqualTo(0);
    assertThat(info.containsDataInTimeRange(1000, 1001)).isTrue();
  }

  @Test
  public void testChunkDataInRange() {
    final LocalDateTime startTime = LocalDateTime.of(2020, 10, 1, 10, 10, 0);
    final long chunkCreationTimeSecs = startTime.toEpochSecond(ZoneOffset.UTC);
    final ChunkInfo info = new ChunkInfo(testChunkName, chunkCreationTimeSecs);
    assertThat(info.getChunkCreationTimeEpochSecs()).isEqualTo(chunkCreationTimeSecs);
    assertThat(info.getChunkLastUpdatedTimeSecsEpochSecs()).isEqualTo(chunkCreationTimeSecs);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(0);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(0);
    assertThat(info.getChunkSnapshotTimeEpochSecs()).isEqualTo(0);

    // Expand the time range for chunk info.
    final long startTimePlus2MinMilli =
        startTime.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli();
    final long startTimeMinus2MinMilli =
        startTime.minusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli();
    info.updateDataTimeRange(startTimeMinus2MinMilli);
    info.updateDataTimeRange(startTimePlus2MinMilli);
    assertThat(info.getDataStartTimeEpochSecs()).isEqualTo(startTimeMinus2MinMilli / 1000);
    assertThat(info.getDataEndTimeEpochSecs()).isEqualTo(startTimePlus2MinMilli / 1000);

    assertThat(info.containsDataInTimeRange(1, 10)).isFalse();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(5).toEpochSecond(ZoneOffset.UTC),
                startTime.minusMinutes(4).toEpochSecond(ZoneOffset.UTC)))
        .isFalse();
    assertThat(
            info.containsDataInTimeRange(
                1, startTime.minusMinutes(2).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(2).toEpochSecond(ZoneOffset.UTC),
                startTime.minusMinutes(2).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(2).toEpochSecond(ZoneOffset.UTC),
                startTime.minusMinutes(1).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(1).toEpochSecond(ZoneOffset.UTC),
                startTime.plusMinutes(1).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(1).toEpochSecond(ZoneOffset.UTC),
                startTime.plusMinutes(2).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(1).toEpochSecond(ZoneOffset.UTC),
                startTime.plusMinutes(3).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(2).toEpochSecond(ZoneOffset.UTC),
                startTime.plusMinutes(3).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(3).toEpochSecond(ZoneOffset.UTC),
                startTime.plusMinutes(5).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusYears(3).toEpochSecond(ZoneOffset.UTC),
                startTime.plusYears(5).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();

    // O length interval
    assertThat(
            info.containsDataInTimeRange(
                startTime.minusMinutes(2).toEpochSecond(ZoneOffset.UTC),
                startTime.minusMinutes(2).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.plusMinutes(2).toEpochSecond(ZoneOffset.UTC),
                startTime.plusMinutes(2).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(
                startTime.plusYears(5).toEpochSecond(ZoneOffset.UTC),
                startTime.plusYears(7).toEpochSecond(ZoneOffset.UTC)))
        .isFalse();
    assertThat(info.containsDataInTimeRange(1, 1)).isFalse();

    // Start time is 0
    assertThat(info.containsDataInTimeRange(0, 0)).isFalse();
    assertThat(
            info.containsDataInTimeRange(
                0, startTime.minusMinutes(3).toEpochSecond(ZoneOffset.UTC)))
        .isFalse();
    assertThat(
            info.containsDataInTimeRange(
                0, startTime.minusMinutes(2).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(0, startTime.plusMinutes(2).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
    assertThat(
            info.containsDataInTimeRange(0, startTime.plusMinutes(3).toEpochSecond(ZoneOffset.UTC)))
        .isTrue();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeStartTimeInDateRange() {
    final ChunkInfo info = new ChunkInfo(testChunkName, 1000);
    info.updateDataTimeRange(980);
    info.updateDataTimeRange(1020);

    assertThat(info.containsDataInTimeRange(-1, 980)).isTrue();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeEndTimeInDateRange() {
    final ChunkInfo info = new ChunkInfo(testChunkName, 1000);
    info.updateDataTimeRange(980);
    info.updateDataTimeRange(1020);

    assertThat(info.containsDataInTimeRange(960, -1)).isTrue();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeIntervalInDateRange() {
    final ChunkInfo info = new ChunkInfo(testChunkName, 1000);
    info.updateDataTimeRange(980);
    info.updateDataTimeRange(1020);

    assertThat(info.containsDataInTimeRange(960, 950)).isTrue();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidChunkName() {
    new ChunkInfo(null, 100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyChunkName() {
    new ChunkInfo("", 100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeChunkCreationTime() {
    new ChunkInfo(testChunkName, -1);
  }
}
