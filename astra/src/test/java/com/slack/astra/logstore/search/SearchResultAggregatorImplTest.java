package com.slack.astra.logstore.search;

import static com.slack.astra.util.AggregatorFactoriesUtil.createDateHistogramAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createDetailedDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.google.common.io.Files;
import com.slack.astra.logstore.DocumentBuilder;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogStore;
import com.slack.astra.logstore.LuceneIndexStoreConfig;
import com.slack.astra.logstore.LuceneIndexStoreImpl;
import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.util.QueryBuilderUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;

public class SearchResultAggregatorImplTest {
  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
  }

  @Test
  public void testSimpleSearchResultsAggWithOneResult() throws IOException {
    long tookMs = 10;
    int bucketCount = 13;
    int howMany = 1;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);

    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    InternalAggregation histogram1 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregation histogram2 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 1, 1, 0, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 0, 1, 1, 0, histogram2);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", histogramStartMs, histogramEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1),
            List.of());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    LogMessage hit = aggSearchResult.hits.get(0);
    assertThat(hit.getId()).contains("Message20");
    assertThat(hit.getTimestamp()).isEqualTo(startTime2.plus(9, ChronoUnit.MINUTES));

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  @Test
  public void testSimpleSearchResultsAggWithMultipleResults() throws IOException {
    long tookMs = 10;
    int bucketCount = 13;
    int howMany = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    InternalAggregation histogram1 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregation histogram2 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 1, 1, 0, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 0, 1, 1, 0, histogram2);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", histogramStartMs, histogramEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "10m", 1),
            List.of());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  @Test
  public void testSearchResultAggregatorOn4Results() throws IOException {
    long tookMs = 10;
    int bucketCount = 25;
    int howMany = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    Instant startTime3 = startTime1.plus(2, ChronoUnit.HOURS);
    Instant startTime4 = startTime1.plus(3, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(4, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);
    List<LogMessage> messages3 =
        MessageUtil.makeMessagesWithTimeDifference(21, 30, 1000 * 60, startTime3);
    List<LogMessage> messages4 =
        MessageUtil.makeMessagesWithTimeDifference(31, 40, 1000 * 60, startTime4);

    InternalAggregation histogram1 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregation histogram2 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));
    InternalAggregation histogram3 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(21, 30, 1000 * 60, startTime3));
    InternalAggregation histogram4 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(31, 40, 1000 * 60, startTime4));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 1, 1, 0, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 1, 1, 1, 1, histogram2);
    SearchResult<LogMessage> searchResult3 =
        new SearchResult<>(messages3, tookMs + 2, 0, 1, 1, 0, histogram3);
    SearchResult<LogMessage> searchResult4 =
        new SearchResult<>(messages4, tookMs + 3, 0, 1, 1, 1, histogram4);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", histogramStartMs, histogramEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1),
            List.of());
    List<SearchResult<LogMessage>> searchResults =
        List.of(searchResult1, searchResult4, searchResult3, searchResult2);
    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 3);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(1);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(4);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages4.contains(m)).isTrue();
    }

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size() + messages3.size() + messages4.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  @Test
  public void testSimpleSearchResultsAggWithNoHistograms() throws IOException {
    long tookMs = 10;
    int howMany = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long searchStartMs = startTime1.toEpochMilli();
    long searchEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            searchStartMs,
            searchEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", searchStartMs, searchEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1),
            List.of());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    assertThat(aggSearchResult.internalAggregation).isNull();
  }

  @Test
  public void testSimpleSearchResultsAggNoHits() throws IOException {
    long tookMs = 10;
    int bucketCount = 13;
    int howMany = 0;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    InternalAggregation histogram1 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregation histogram2 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(Collections.emptyList(), tookMs, 0, 2, 2, 2, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(Collections.emptyList(), tookMs + 1, 0, 1, 1, 0, histogram2);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", histogramStartMs, histogramEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1),
            List.of());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.hits.size()).isZero();
    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(3);

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  @Test
  public void testSearchResultsAggIgnoresBucketsInSearchResultsSafely() throws IOException {
    long tookMs = 10;
    int howMany = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long startTimeMs = startTime1.toEpochMilli();
    long endTimeMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    InternalAggregation histogram1 =
        makeHistogram(
            startTimeMs,
            endTimeMs,
            "6m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 1, 1, 1, 0, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            startTimeMs,
            endTimeMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", startTimeMs, endTimeMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1),
            List.of());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, false);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(1);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(internalDateHistogram).isEqualTo(histogram1);
  }

  @Test
  public void testSimpleSearchResultsAggIgnoreHitsSafely() throws IOException {
    long tookMs = 10;
    int bucketCount = 13;
    int howMany = 0;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    InternalAggregation histogram1 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregation histogram2 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 2, 2, 2, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(Collections.emptyList(), tookMs + 1, 0, 1, 1, 0, histogram2);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", histogramStartMs, histogramEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "10m", 1),
            List.of());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.hits.size()).isZero();
    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(3);

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  /**
   * Makes an InternalDateHistogram given the provided configuration. Since the
   * InternalDateHistogram has private constructors this uses a temporary LogSearcher to index,
   * search, and then collect the results into an appropriate aggregation.
   */
  private InternalAggregation makeHistogram(
      long histogramStartMs, long histogramEndMs, String interval, List<Trace.Span> logMessages)
      throws IOException {
    File tempFolder = Files.createTempDir();
    LuceneIndexStoreConfig indexStoreCfg =
        new LuceneIndexStoreConfig(
            Duration.of(1, ChronoUnit.MINUTES),
            Duration.of(1, ChronoUnit.MINUTES),
            tempFolder.getCanonicalPath(),
            false);
    MeterRegistry metricsRegistry = new SimpleMeterRegistry();
    DocumentBuilder documentBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD,
            true,
            metricsRegistry);

    LogStore logStore = new LuceneIndexStoreImpl(indexStoreCfg, documentBuilder, metricsRegistry);
    LogIndexSearcherImpl logSearcher =
        new LogIndexSearcherImpl(logStore.getAstraSearcherManager(), logStore.getSchema());

    for (Trace.Span logMessage : logMessages) {
      logStore.addMessage(logMessage);
    }
    logStore.commit();
    logStore.refresh();

    SearchResult<LogMessage> messageSearchResult =
        logSearcher.search(
            "testDataSet",
            0,
            QueryBuilderUtil.generateQueryBuilder("*:*", histogramStartMs, histogramEndMs),
            null,
            createDetailedDateHistogramAggregatorFactoriesBuilder(
                "1",
                LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                interval,
                0,
                histogramStartMs,
                histogramEndMs),
            List.of());

    try {
      return messageSearchResult.internalAggregation;
    } finally {
      logSearcher.close();
      logStore.close();
      logStore.cleanup();
    }
  }

  @Test
  public void testDistributedSortByTimestampDescending() {
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(10, ChronoUnit.SECONDS);

    // Create messages from two different chunks with interleaved timestamps
    List<LogMessage> messages1 =
        List.of(
            MessageUtil.makeMessage(1, startTime1),
            MessageUtil.makeMessage(3, startTime1.plusSeconds(4)));

    List<LogMessage> messages2 =
        List.of(
            MessageUtil.makeMessage(2, startTime2.plusSeconds(2)),
            MessageUtil.makeMessage(4, startTime2.plusSeconds(6)));

    SearchResult<LogMessage> searchResult1 = new SearchResult<>(messages1, 10, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 = new SearchResult<>(messages2, 10, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            startTime1.toEpochMilli(),
            startTime2.plusSeconds(10).toEpochMilli(),
            4,
            Collections.emptyList(),
            null,
            null,
            null,
            List.of(
                new SearchQuery.SortSpec(
                    LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, true, "boolean")));

    SearchResult<LogMessage> aggResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), false);

    assertThat(aggResult.hits.size()).isEqualTo(4);
    // Should be sorted by timestamp descending (newest first)
    assertThat(aggResult.hits.get(0).getId()).isEqualTo("Message4");
    assertThat(aggResult.hits.get(1).getId()).isEqualTo("Message2");
    assertThat(aggResult.hits.get(2).getId()).isEqualTo("Message3");
    assertThat(aggResult.hits.get(3).getId()).isEqualTo("Message1");
  }

  @Test
  public void testDistributedSortByCustomFieldAscending() {
    Instant time = Instant.now();

    // Create messages with custom longproperty values across two chunks
    List<LogMessage> messages1 =
        List.of(
            MessageUtil.makeMessage(1, time, Map.of("longproperty", 300L)),
            MessageUtil.makeMessage(2, time, Map.of("longproperty", 100L)));

    List<LogMessage> messages2 =
        List.of(
            MessageUtil.makeMessage(3, time, Map.of("longproperty", 200L)),
            MessageUtil.makeMessage(4, time, Map.of("longproperty", 50L)));

    SearchResult<LogMessage> searchResult1 = new SearchResult<>(messages1, 10, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 = new SearchResult<>(messages2, 10, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            0L,
            Long.MAX_VALUE,
            4,
            Collections.emptyList(),
            null,
            null,
            null,
            List.of(new SearchQuery.SortSpec("longproperty", false, "boolean")));

    SearchResult<LogMessage> aggResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), false);

    assertThat(aggResult.hits.size()).isEqualTo(4);
    // Should be sorted by longproperty ascending
    assertThat(aggResult.hits.get(0).getSource().get("longproperty")).isEqualTo(50L);
    assertThat(aggResult.hits.get(1).getSource().get("longproperty")).isEqualTo(100L);
    assertThat(aggResult.hits.get(2).getSource().get("longproperty")).isEqualTo(200L);
    assertThat(aggResult.hits.get(3).getSource().get("longproperty")).isEqualTo(300L);
  }

  @Test
  public void testDistributedSortByMultipleFields() {
    Instant time = Instant.now();

    // Create messages with same string but different long values across chunks
    List<LogMessage> messages1 =
        List.of(
            MessageUtil.makeMessage(
                1, time, Map.of("stringproperty", "apple", "longproperty", 300L)),
            MessageUtil.makeMessage(
                2, time, Map.of("stringproperty", "banana", "longproperty", 100L)));

    List<LogMessage> messages2 =
        List.of(
            MessageUtil.makeMessage(
                3, time, Map.of("stringproperty", "apple", "longproperty", 50L)),
            MessageUtil.makeMessage(
                4, time, Map.of("stringproperty", "banana", "longproperty", 200L)));

    SearchResult<LogMessage> searchResult1 = new SearchResult<>(messages1, 10, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 = new SearchResult<>(messages2, 10, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            0L,
            Long.MAX_VALUE,
            4,
            Collections.emptyList(),
            null,
            null,
            null,
            List.of(
                new SearchQuery.SortSpec("stringproperty", false, "boolean"),
                new SearchQuery.SortSpec("longproperty", true, "boolean")));

    SearchResult<LogMessage> aggResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), false);

    assertThat(aggResult.hits.size()).isEqualTo(4);
    // First two should be "apple" sorted by long descending (300, 50)
    assertThat(aggResult.hits.get(0).getSource().get("stringproperty")).isEqualTo("apple");
    assertThat(aggResult.hits.get(0).getSource().get("longproperty")).isEqualTo(300L);
    assertThat(aggResult.hits.get(1).getSource().get("stringproperty")).isEqualTo("apple");
    assertThat(aggResult.hits.get(1).getSource().get("longproperty")).isEqualTo(50L);
    // Next two should be "banana" sorted by long descending (200, 100)
    assertThat(aggResult.hits.get(2).getSource().get("stringproperty")).isEqualTo("banana");
    assertThat(aggResult.hits.get(2).getSource().get("longproperty")).isEqualTo(200L);
    assertThat(aggResult.hits.get(3).getSource().get("stringproperty")).isEqualTo("banana");
    assertThat(aggResult.hits.get(3).getSource().get("longproperty")).isEqualTo(100L);
  }

  @Test
  public void testDistributedSortWithMissingValues() {
    Instant time = Instant.now();

    // Create messages where some have the sort field and some don't
    // Use custom_field instead of longproperty since makeMessage always adds default longproperty
    List<LogMessage> messages1 =
        List.of(
            MessageUtil.makeMessage(1, time, Map.of("custom_field", 300L)),
            MessageUtil.makeMessage(2, time)); // No custom_field

    List<LogMessage> messages2 =
        List.of(
            MessageUtil.makeMessage(3, time, Map.of("custom_field", 100L)),
            MessageUtil.makeMessage(4, time)); // No custom_field

    SearchResult<LogMessage> searchResult1 = new SearchResult<>(messages1, 10, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 = new SearchResult<>(messages2, 10, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            0L,
            Long.MAX_VALUE,
            4,
            Collections.emptyList(),
            null,
            null,
            null,
            List.of(new SearchQuery.SortSpec("custom_field", false, "boolean")));

    SearchResult<LogMessage> aggResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), false);

    assertThat(aggResult.hits.size()).isEqualTo(4);
    // First two should have the field (100, 300)
    assertThat(aggResult.hits.get(0).getSource().get("custom_field")).isEqualTo(100L);
    assertThat(aggResult.hits.get(1).getSource().get("custom_field")).isEqualTo(300L);
    // Last two should not have the field (nulls go to end)
    assertThat(aggResult.hits.get(2).getSource().containsKey("custom_field")).isFalse();
    assertThat(aggResult.hits.get(3).getSource().containsKey("custom_field")).isFalse();
  }

  @Test
  public void testDistributedSortRespectTopN() {
    Instant time = Instant.now();

    // Create many messages across multiple chunks
    List<LogMessage> messages1 =
        List.of(
            MessageUtil.makeMessage(1, time, Map.of("longproperty", 100L)),
            MessageUtil.makeMessage(2, time, Map.of("longproperty", 200L)),
            MessageUtil.makeMessage(3, time, Map.of("longproperty", 300L)));

    List<LogMessage> messages2 =
        List.of(
            MessageUtil.makeMessage(4, time, Map.of("longproperty", 50L)),
            MessageUtil.makeMessage(5, time, Map.of("longproperty", 150L)),
            MessageUtil.makeMessage(6, time, Map.of("longproperty", 250L)));

    SearchResult<LogMessage> searchResult1 = new SearchResult<>(messages1, 10, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 = new SearchResult<>(messages2, 10, 0, 1, 1, 0, null);

    // Request only top 3 results
    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            0L,
            Long.MAX_VALUE,
            3,
            Collections.emptyList(),
            null,
            null,
            null,
            List.of(new SearchQuery.SortSpec("longproperty", false, "boolean")));

    SearchResult<LogMessage> aggResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), false);

    // Should only return top 3 after sorting
    assertThat(aggResult.hits.size()).isEqualTo(3);
    assertThat(aggResult.hits.get(0).getSource().get("longproperty")).isEqualTo(50L);
    assertThat(aggResult.hits.get(1).getSource().get("longproperty")).isEqualTo(100L);
    assertThat(aggResult.hits.get(2).getSource().get("longproperty")).isEqualTo(150L);
  }

  @Test
  public void testDistributedDefaultSortWhenNoSortFieldsProvided() {
    Instant startTime = Instant.now();

    // Create messages with different timestamps
    List<LogMessage> messages1 =
        List.of(
            MessageUtil.makeMessage(1, startTime),
            MessageUtil.makeMessage(2, startTime.plusSeconds(4)));

    List<LogMessage> messages2 =
        List.of(
            MessageUtil.makeMessage(3, startTime.plusSeconds(2)),
            MessageUtil.makeMessage(4, startTime.plusSeconds(6)));

    SearchResult<LogMessage> searchResult1 = new SearchResult<>(messages1, 10, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 = new SearchResult<>(messages2, 10, 0, 1, 1, 0, null);

    // Empty sort fields should default to timestamp descending
    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            0L,
            Long.MAX_VALUE,
            4,
            Collections.emptyList(),
            null,
            null,
            null,
            List.of());

    SearchResult<LogMessage> aggResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), false);

    assertThat(aggResult.hits.size()).isEqualTo(4);
    // Should be sorted by timestamp descending (newest first)
    assertThat(aggResult.hits.get(0).getId()).isEqualTo("Message4");
    assertThat(aggResult.hits.get(1).getId()).isEqualTo("Message2");
    assertThat(aggResult.hits.get(2).getId()).isEqualTo("Message3");
    assertThat(aggResult.hits.get(3).getId()).isEqualTo("Message1");
  }
}
