package com.slack.kaldb.chunk;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;

class LogSearcherQueryTest {

  @Test
  public void shouldUseOptimizedQueryStartEndTime() {
    // Query is before the chunk data, so do not return a start time
    assertThat(LogSearcherQuery.getStartTime(10, 12)).isNull();

    // Query matches chunk start time, do not return a start time
    assertThat(LogSearcherQuery.getStartTime(10, 10)).isNull();

    // Query only matches part of the chunk, return the query start time
    assertThat(LogSearcherQuery.getStartTime(10, 9)).isEqualTo(10);

    // Query only matches part of the chunk, return the query end time
    assertThat(LogSearcherQuery.getEndTime(10, 12)).isEqualTo(10);

    // Query matches chunk end time, do not return an end time
    assertThat(LogSearcherQuery.getEndTime(10, 10)).isNull();

    // Query is after the chunk data, so do not return an end time
    assertThat(LogSearcherQuery.getEndTime(12, 10)).isNull();
  }
}
