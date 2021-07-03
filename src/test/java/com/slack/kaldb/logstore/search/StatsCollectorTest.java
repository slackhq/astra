package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_COUNTER;
import static com.slack.kaldb.testlib.MessageUtil.TEST_INDEX_NAME;
import static com.slack.kaldb.testlib.MessageUtil.makeMessageWithIndexAndTimestamp;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.histogram.HistogramBucket;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.IOException;
import java.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class StatsCollectorTest {
  @Rule
  public TemporaryLogStoreAndSearcherRule strictLogStore =
      new TemporaryLogStoreAndSearcherRule(false);

  public StatsCollectorTest() throws IOException {}

  @Test
  public void testStatsCollectorWithPerMinuteMessages() {
    Instant time = Instant.ofEpochSecond(1593365471);
    LogMessage m1 = makeMessageWithIndexAndTimestamp(1, "apple", TEST_INDEX_NAME, time);
    LogMessage m2 =
        makeMessageWithIndexAndTimestamp(2, "baby", TEST_INDEX_NAME, time.plusSeconds(60));
    LogMessage m3 =
        makeMessageWithIndexAndTimestamp(
            3, "apple baby", TEST_INDEX_NAME, time.plusSeconds(2 * 60));
    LogMessage m4 =
        makeMessageWithIndexAndTimestamp(4, "car", TEST_INDEX_NAME, time.plusSeconds(3 * 60));
    LogMessage m5 =
        makeMessageWithIndexAndTimestamp(
            5, "apple baby car", TEST_INDEX_NAME, time.plusSeconds(4 * 60));
    strictLogStore.logStore.addMessage(m1);
    strictLogStore.logStore.addMessage(m2);
    strictLogStore.logStore.addMessage(m3);
    strictLogStore.logStore.addMessage(m4);
    strictLogStore.logStore.addMessage(m5);
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_INDEX_NAME,
            "",
            time.toEpochMilli(),
            time.plusSeconds(4 * 60).toEpochMilli(),
            0,
            5);

    assertThat(allIndexItems.hits.size()).isEqualTo(0);
    assertThat(allIndexItems.totalCount).isEqualTo(5);
    assertThat(allIndexItems.buckets.size()).isEqualTo(5);

    for (HistogramBucket bucket : allIndexItems.buckets) {
      // TODO: Test bucket start and end ranges.
      assertThat(bucket.getCount()).isEqualTo(1);
    }

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(5);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getCount(REFRESHES_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getCount(COMMITS_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);
  }
}
