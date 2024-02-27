package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.kaldb.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.kaldb.testlib.MessageUtil.makeMessageWithIndexAndTimestamp;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getTimerCount;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;

public class StatsCollectorTest {
  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension strictLogStore =
      new TemporaryLogStoreAndSearcherExtension(true);

  public StatsCollectorTest() throws IOException {}

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
  }

  @Test
  public void testStatsCollectorWithPerMinuteMessages() {
    Instant time = Instant.ofEpochSecond(1593365471);
    LogMessage m1 = makeMessageWithIndexAndTimestamp(1, "apple", TEST_DATASET_NAME, time);
    LogMessage m2 =
        makeMessageWithIndexAndTimestamp(2, "baby", TEST_DATASET_NAME, time.plusSeconds(60));
    LogMessage m3 =
        makeMessageWithIndexAndTimestamp(
            3, "apple baby", TEST_DATASET_NAME, time.plusSeconds(2 * 60));
    LogMessage m4 =
        makeMessageWithIndexAndTimestamp(4, "car", TEST_DATASET_NAME, time.plusSeconds(3 * 60));
    LogMessage m5 =
        makeMessageWithIndexAndTimestamp(
            5, "apple baby car", TEST_DATASET_NAME, time.plusSeconds(4 * 60));
    strictLogStore.logStore.addMessage(MessageUtil.convertLogMessageToSpan(m1));
    strictLogStore.logStore.addMessage(MessageUtil.convertLogMessageToSpan(m2));
    strictLogStore.logStore.addMessage(MessageUtil.convertLogMessageToSpan(m3));
    strictLogStore.logStore.addMessage(MessageUtil.convertLogMessageToSpan(m4));
    strictLogStore.logStore.addMessage(MessageUtil.convertLogMessageToSpan(m5));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            time.toEpochMilli(),
            time.plusSeconds(4 * 60).toEpochMilli(),
            0,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));

    assertThat(allIndexItems.hits.size()).isEqualTo(0);

    InternalDateHistogram dateHistogram = (InternalDateHistogram) allIndexItems.internalAggregation;
    assertThat(Objects.requireNonNull(dateHistogram).getBuckets().size()).isEqualTo(5);
    for (InternalDateHistogram.Bucket bucket : Objects.requireNonNull(dateHistogram).getBuckets()) {
      assertThat(bucket.getDocCount()).isEqualTo(1);
    }

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(5);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
  }
}
