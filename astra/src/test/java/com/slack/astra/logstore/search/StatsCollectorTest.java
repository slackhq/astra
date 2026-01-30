package com.slack.astra.logstore.search;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.astra.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.MetricsUtil.getTimerCount;
import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension;
import com.slack.astra.util.QueryBuilderUtil;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
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
  public void testStatsCollectorWithPerMinuteMessages() throws IOException {
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, time.plusSeconds(60)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(3, time.plusSeconds(2 * 60)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(4, time.plusSeconds(3 * 60)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(5, time.plusSeconds(4 * 60)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            0,
            QueryBuilderUtil.generateQueryBuilder(
                "", time.toEpochMilli(), time.plusSeconds(4 * 60).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder(),
            List.of());

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
