package com.slack.kaldb.logstore;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import java.time.Duration;
import org.junit.jupiter.api.Test;

public class LuceneIndexStoreConfigTest {
  @Test
  public void testZeroCommitDuration() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new LuceneIndexStoreConfig(
                    Duration.ZERO, Duration.ofSeconds(10), "indexRoot", "logfile", true, false));
  }

  @Test
  public void testZeroRefreshDuration() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new LuceneIndexStoreConfig(
                    Duration.ofSeconds(10), Duration.ZERO, "indexRoot", "logfile", true, false));
  }

  @Test
  public void testNegativeCommitDuration() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new LuceneIndexStoreConfig(
                    Duration.ofSeconds(-10),
                    Duration.ofSeconds(10),
                    "indexRoot",
                    "logfile",
                    true,
                    false));
  }

  @Test
  public void testNegativeRefreshDuration() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new LuceneIndexStoreConfig(
                    Duration.ofSeconds(10),
                    Duration.ofSeconds(-100),
                    "indexRoot",
                    "logfile",
                    true,
                    false));
  }
}
