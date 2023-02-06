package com.slack.kaldb.logstore.search;

import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
<<<<<<< bburkholder/opensearch-serialize
import com.google.common.io.Files;
import com.slack.kaldb.logstore.DocumentBuilder;
=======
>>>>>>> Initial cleanup
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogStore;
import com.slack.kaldb.logstore.LuceneIndexStoreConfig;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.logstore.opensearch.OpenSearchAggregationAdapter;
import com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;

public class SearchResultAggregatorImplTest {
<<<<<<< bburkholder/opensearch-serialize
=======
  private SearchResult<LogMessage> makeSearchResult(
      List<LogMessage> messages,
      long tookMs,
      long totalCount,
      List<Object> buckets,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas) {
    return new SearchResult<>(
        messages,
        tookMs,
        totalCount,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshotsWithReplicas,
        null);
  }

>>>>>>> Test aggs all the way out
  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
  }

  @Test
  public void testSimpleSearchResultsAggWithOneResult() throws IOException {
    long tookMs = 10;
    int bucketCount = 12;
    int howMany = 1;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = LocalDateTime.of(2020, 1, 1, 2, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);

    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

<<<<<<< bburkholder/opensearch-serialize
    InternalAggregation histogram1 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages1);
    InternalAggregation histogram2 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages2);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 10, 0, 1, 1, 0, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 10, 0, 1, 1, 0, histogram2);
=======
    //    Histogram histogram1 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages1);
    //    Histogram histogram2 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages2);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(messages1, tookMs, 10, List.of(), 0, 1, 1, 0);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(messages2, tookMs + 1, 10, List.of(), 0, 1, 1, 0);
>>>>>>> Initial cleanup

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            "Message1",
            histogramStartMs,
            histogramEndMs,
            howMany,
            bucketCount,
            Collections.emptyList());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    LogMessage hit = aggSearchResult.hits.get(0);
    assertThat(hit.id).contains("Message20");
    assertThat(hit.timeSinceEpochMilli)
        .isEqualTo(startTime2.plus(9, ChronoUnit.MINUTES).toEpochMilli());

    assertThat(aggSearchResult.totalCount).isEqualTo(20);
<<<<<<< bburkholder/opensearch-serialize

    InternalAutoDateHistogram internalAutoDateHistogram =
        Objects.requireNonNull((InternalAutoDateHistogram) aggSearchResult.internalAggregation);
    assertThat(internalAutoDateHistogram.getTargetBuckets()).isEqualTo(bucketCount);
    assertThat(
            internalAutoDateHistogram
                .getBuckets()
                .stream()
                .collect(Collectors.summarizingLong(InternalAutoDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
=======
    //    for (HistogramBucket b : aggSearchResult.buckets) {
    //      assertThat(b.getCount() == 10 || b.getCount() == 0).isTrue();
    //    }
>>>>>>> Initial cleanup
  }

  @Test
  public void testSimpleSearchResultsAggWithMultipleResults() throws IOException {
    long tookMs = 10;
    int bucketCount = 12;
    int howMany = 10;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

<<<<<<< bburkholder/opensearch-serialize
    InternalAggregation histogram1 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages1);
    InternalAggregation histogram2 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages2);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 10, 0, 1, 1, 0, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 10, 0, 1, 1, 0, histogram2);
=======
    //    Histogram histogram1 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages1);
    //    Histogram histogram2 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages2);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(messages1, tookMs, 10, List.of(), 0, 1, 1, 0);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(messages2, tookMs + 1, 10, List.of(), 0, 1, 1, 0);
>>>>>>> Initial cleanup

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            "Message1",
            histogramStartMs,
            histogramEndMs,
            howMany,
            bucketCount,
            Collections.emptyList());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    assertThat(aggSearchResult.totalCount).isEqualTo(20);
<<<<<<< bburkholder/opensearch-serialize

    InternalAutoDateHistogram internalAutoDateHistogram =
        Objects.requireNonNull((InternalAutoDateHistogram) aggSearchResult.internalAggregation);
    assertThat(internalAutoDateHistogram.getTargetBuckets()).isEqualTo(bucketCount);
    assertThat(
            internalAutoDateHistogram
                .getBuckets()
                .stream()
                .collect(Collectors.summarizingLong(InternalAutoDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
=======
    //    for (HistogramBucket b : aggSearchResult.buckets) {
    //      assertThat(b.getCount() == 10 || b.getCount() == 0).isTrue();
    //    }
>>>>>>> Initial cleanup
  }

  @Test
  public void testSearchResultAggregatorOn4Results() throws IOException {
    long tookMs = 10;
    int bucketCount = 24;
    int howMany = 10;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
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

<<<<<<< bburkholder/opensearch-serialize
    InternalAggregation histogram1 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages1);
    InternalAggregation histogram2 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages2);
    InternalAggregation histogram3 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages3);
    InternalAggregation histogram4 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages4);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 10, 0, 1, 1, 0, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 10, 1, 1, 1, 1, histogram2);
    SearchResult<LogMessage> searchResult3 =
        new SearchResult<>(messages3, tookMs + 2, 10, 0, 1, 1, 0, histogram3);
    SearchResult<LogMessage> searchResult4 =
        new SearchResult<>(messages4, tookMs + 3, 10, 0, 1, 1, 1, histogram4);
=======
    //    Histogram histogram1 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages1);
    //    Histogram histogram2 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages2);
    //    Histogram histogram3 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages3);
    //    Histogram histogram4 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages4);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(messages1, tookMs, 10, List.of(), 0, 1, 1, 0);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(messages2, tookMs + 1, 10, List.of(), 1, 1, 1, 1);
    SearchResult<LogMessage> searchResult3 =
        makeSearchResult(messages3, tookMs + 2, 10, List.of(), 0, 1, 1, 0);
    SearchResult<LogMessage> searchResult4 =
        makeSearchResult(messages4, tookMs + 3, 10, List.of(), 0, 1, 1, 1);
>>>>>>> Initial cleanup

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            "Message1",
            histogramStartMs,
            histogramEndMs,
            howMany,
            bucketCount,
            Collections.emptyList());
    List<SearchResult<LogMessage>> searchResults =
        List.of(searchResult1, searchResult4, searchResult3, searchResult2);
    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 3);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(1);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(4);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages4.contains(m)).isTrue();
    }

    assertThat(aggSearchResult.totalCount).isEqualTo(40);
<<<<<<< bburkholder/opensearch-serialize

    InternalAutoDateHistogram internalAutoDateHistogram =
        Objects.requireNonNull((InternalAutoDateHistogram) aggSearchResult.internalAggregation);
    assertThat(internalAutoDateHistogram.getTargetBuckets()).isEqualTo(bucketCount);
    assertThat(
            internalAutoDateHistogram
                .getBuckets()
                .stream()
                .collect(Collectors.summarizingLong(InternalAutoDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size() + messages3.size() + messages4.size());
=======
    //    for (HistogramBucket b : aggSearchResult.buckets) {
    //      assertThat(b.getCount() == 10 || b.getCount() == 0).isTrue();
    //    }
>>>>>>> Initial cleanup
  }

  @Test
  public void testSimpleSearchResultsAggWithNoHistograms() throws IOException {
    long tookMs = 10;
    int bucketCount = 0;
    int howMany = 10;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long searchStartMs = startTime1.toEpochMilli();
    long searchEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 10, 0, 1, 1, 0, emptyAggregation());
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 10, 0, 1, 1, 0, emptyAggregation());

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            "Message1",
            searchStartMs,
            searchEndMs,
            howMany,
            bucketCount,
            Collections.emptyList());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    assertThat(aggSearchResult.totalCount).isEqualTo(20);
<<<<<<< bburkholder/opensearch-serialize

    InternalAutoDateHistogram internalAutoDateHistogram =
        Objects.requireNonNull((InternalAutoDateHistogram) aggSearchResult.internalAggregation);
    assertThat(internalAutoDateHistogram.getBuckets().size()).isEqualTo(0);
=======
    //    assertThat(aggSearchResult.buckets.size()).isZero();
>>>>>>> Initial cleanup
  }

  @Test
  public void testSimpleSearchResultsAggNoHits() throws IOException {
    long tookMs = 10;
    int bucketCount = 12;
    int howMany = 0;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

<<<<<<< bburkholder/opensearch-serialize
    InternalAggregation histogram1 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages1);
    InternalAggregation histogram2 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages2);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(Collections.emptyList(), tookMs, 7, 0, 2, 2, 2, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(Collections.emptyList(), tookMs + 1, 8, 0, 1, 1, 0, histogram2);
=======
    //    Histogram histogram1 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages1);
    //    Histogram histogram2 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages2);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(Collections.emptyList(), tookMs, 7, List.of(), 0, 2, 2, 2);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(Collections.emptyList(), tookMs + 1, 8, List.of(), 0, 1, 1, 0);
>>>>>>> Initial cleanup

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            "Message1",
            histogramStartMs,
            histogramEndMs,
            howMany,
            bucketCount,
            Collections.emptyList());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults);

    assertThat(aggSearchResult.hits.size()).isZero();
    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(3);
    assertThat(aggSearchResult.totalCount).isEqualTo(15);
<<<<<<< bburkholder/opensearch-serialize

    InternalAutoDateHistogram internalAutoDateHistogram =
        Objects.requireNonNull((InternalAutoDateHistogram) aggSearchResult.internalAggregation);
    assertThat(internalAutoDateHistogram.getTargetBuckets()).isEqualTo(bucketCount);
    assertThat(
            internalAutoDateHistogram
                .getBuckets()
                .stream()
                .collect(Collectors.summarizingLong(InternalAutoDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
=======
    //    for (HistogramBucket b : aggSearchResult.buckets) {
    //      assertThat(b.getCount() == 10 || b.getCount() == 0).isTrue();
    //    }
>>>>>>> Initial cleanup
  }

  @Test
  public void testSearchResultsAggIgnoresBucketsInSearchResultsSafely() throws IOException {
    long tookMs = 10;
    int bucketCount = 0;
    int howMany = 10;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long startTimeMs = startTime1.toEpochMilli();
    long endTimeMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

<<<<<<< bburkholder/opensearch-serialize
    InternalAggregation histogram1 = makeHistogram(startTimeMs, endTimeMs, 2, messages1);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 10, 1, 1, 1, 0, histogram1);
=======
    //    Histogram histogram1 = makeHistogram(startTimeMs, endTimeMs, 2, messages1);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(messages1, tookMs, 10, List.of(), 1, 1, 1, 0);
>>>>>>> Initial cleanup
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 11, 0, 1, 1, 0, emptyAggregation());

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            "Message1",
            startTimeMs,
            endTimeMs,
            howMany,
            bucketCount,
            Collections.emptyList());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(1);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    assertThat(aggSearchResult.totalCount).isEqualTo(21);
<<<<<<< bburkholder/opensearch-serialize

    InternalAutoDateHistogram internalAutoDateHistogram =
        Objects.requireNonNull((InternalAutoDateHistogram) aggSearchResult.internalAggregation);
    assertThat(internalAutoDateHistogram).isEqualTo(histogram1);
=======
    //    assertThat(aggSearchResult.buckets.size()).isZero();
>>>>>>> Initial cleanup
  }

  @Test
  public void testSimpleSearchResultsAggIgnoreHitsSafely() throws IOException {
    long tookMs = 10;
    int bucketCount = 12;
    int howMany = 0;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

<<<<<<< bburkholder/opensearch-serialize
    InternalAggregation histogram1 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages1);
    InternalAggregation histogram2 =
        makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages2);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 7, 0, 2, 2, 2, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(Collections.emptyList(), tookMs + 1, 8, 0, 1, 1, 0, histogram2);
=======
    //    Histogram histogram1 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages1);
    //    Histogram histogram2 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount,
    // messages2);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(messages1, tookMs, 7, List.of(), 0, 2, 2, 2);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(Collections.emptyList(), tookMs + 1, 8, List.of(), 0, 1, 1, 0);
>>>>>>> Initial cleanup

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            "Message1",
            histogramStartMs,
            histogramEndMs,
            howMany,
            bucketCount,
            Collections.emptyList());
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults);

    assertThat(aggSearchResult.hits.size()).isZero();
    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(3);
    assertThat(aggSearchResult.totalCount).isEqualTo(15);
<<<<<<< bburkholder/opensearch-serialize

    InternalAutoDateHistogram internalAutoDateHistogram =
        Objects.requireNonNull((InternalAutoDateHistogram) aggSearchResult.internalAggregation);
    assertThat(internalAutoDateHistogram.getTargetBuckets()).isEqualTo(bucketCount);
    assertThat(
            internalAutoDateHistogram
                .getBuckets()
                .stream()
                .collect(Collectors.summarizingLong(InternalAutoDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
  }

  private InternalAutoDateHistogram emptyAggregation() throws IOException {
    return (InternalAutoDateHistogram)
        OpenSearchAggregationAdapter.buildAutoDateHistogramAggregator(1).buildEmptyAggregation();
  }

  /**
   * Makes an InternalAutoDateHistogram given the provided configuration. Since the
   * InternalAutoDateHistogram has private constructors this uses a temporary LogSearcher to index,
   * search, and then collect the results into an appropriate aggregation.
   */
  private InternalAggregation makeHistogram(
      long histogramStartMs, long histogramEndMs, int bucketCount, List<LogMessage> logMessages)
      throws IOException {
    File tempFolder = Files.createTempDir();
    LuceneIndexStoreConfig indexStoreCfg =
        new LuceneIndexStoreConfig(
            Duration.of(1, ChronoUnit.MINUTES),
            Duration.of(1, ChronoUnit.MINUTES),
            tempFolder.getCanonicalPath(),
            false);
    MeterRegistry metricsRegistry = new SimpleMeterRegistry();
    DocumentBuilder<LogMessage> documentBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD,
            true,
            metricsRegistry);

    LogStore<LogMessage> logStore =
        new LuceneIndexStoreImpl(indexStoreCfg, documentBuilder, metricsRegistry);
    LogIndexSearcherImpl logSearcher =
        new LogIndexSearcherImpl(logStore.getSearcherManager(), logStore.getSchema());

    for (LogMessage logMessage : logMessages) {
      logStore.addMessage(logMessage);
    }
    logStore.commit();
    logStore.refresh();

    SearchResult<LogMessage> messageSearchResult =
        logSearcher.search("testDataSet", "*:*", histogramStartMs, histogramEndMs, 0, bucketCount);

    try {
      return messageSearchResult.internalAggregation;
    } finally {
      logSearcher.close();
      logStore.close();
      logStore.cleanup();
    }
=======
    //    for (HistogramBucket b : aggSearchResult.buckets) {
    //      assertThat(b.getCount() == 10 || b.getCount() == 0).isTrue();
    //    }
>>>>>>> Initial cleanup
  }
}
