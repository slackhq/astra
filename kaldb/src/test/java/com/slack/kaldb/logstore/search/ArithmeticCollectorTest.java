package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.kaldb.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getTimerCount;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.kaldb.histogram.HistogramBucket;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ArithmeticCollectorTest {
  @Rule
  public TemporaryLogStoreAndSearcherRule strictLogStore =
      new TemporaryLogStoreAndSearcherRule(false);

  public ArithmeticCollectorTest() throws IOException {}

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
  }

  @Test
  public void testArithmeticCollector() {
    Instant time = Instant.ofEpochSecond(1593365471);
    LogMessage m1 = makeMessage(1, "apple", time);
    LogMessage m2 = makeMessage(2, "baby", time.plusSeconds(60));
    LogMessage m3 = makeMessage(3, "apple baby", time.plusSeconds(2 * 60));
    LogMessage m4 = makeMessage(4, "car", time.plusSeconds(3 * 60));
    LogMessage m5 = makeMessage(5, "apple baby car", time.plusSeconds(4 * 60));
    strictLogStore.logStore.addMessage(m1);
    strictLogStore.logStore.addMessage(m2);
    strictLogStore.logStore.addMessage(m3);
    strictLogStore.logStore.addMessage(m4);
    strictLogStore.logStore.addMessage(m5);
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            time.toEpochMilli(),
            time.plusSeconds(4 * 60).toEpochMilli(),
            0,
            5,
            List.of(
                new AggregationDefinition(
                    "test", AggregationDefinition.AggregationType.SUM, "int_prop")));

    assertThat(allIndexItems.hits.size()).isEqualTo(0);
    assertThat(allIndexItems.totalCount).isEqualTo(5);
    assertThat(allIndexItems.buckets.size()).isEqualTo(5);

    for (HistogramBucket bucket : allIndexItems.buckets) {
      // TODO: Test bucket start and end ranges.
      assertThat(bucket.getCount()).isEqualTo(1);
    }

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(5);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
  }

  public static LogMessage makeMessage(int i, String message, Instant timestamp) {
    String id = "DEFAULT_MESSAGE_PREFIX" + i;
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, timestamp.toString());
    fieldMap.put(LogMessage.ReservedField.MESSAGE.fieldName, message);
    fieldMap.put("int_prop", i);
    fieldMap.put("long_prop", (long) i);
    fieldMap.put("double_prop", (double) i);
    fieldMap.put("float_prop", (float) i);
    return LogMessage.fromWireMessage(new LogWireMessage(TEST_DATASET_NAME, "INFO", id, fieldMap));
  }
}
