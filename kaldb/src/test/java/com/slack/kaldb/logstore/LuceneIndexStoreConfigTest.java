package com.slack.kaldb.logstore;

import java.time.Duration;
import org.junit.Test;

public class LuceneIndexStoreConfigTest {
  @Test(expected = IllegalArgumentException.class)
  public void testZeroCommitDuration() {
    new LuceneIndexStoreConfig(
        Duration.ZERO, Duration.ofSeconds(10), "indexRoot", "logfile", 20, true);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testZeroRefreshDuration() {
    new LuceneIndexStoreConfig(
        Duration.ofSeconds(10), Duration.ZERO, "indexRoot", "logfile", 20, true);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeCommitDuration() {
    new LuceneIndexStoreConfig(
        Duration.ofSeconds(-10), Duration.ofSeconds(10), "indexRoot", "logfile", 20, true);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeRefreshDuration() {
    new LuceneIndexStoreConfig(
        Duration.ofSeconds(10), Duration.ofSeconds(-100), "indexRoot", "logfile", 20, true);
  }
}
