package com.slack.kaldb.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.aggregations.AggBuilder;
import com.slack.kaldb.logstore.search.aggregations.AggBuilderBase;
import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.HistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.UniqueCountAggBuilder;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.CollectorManager;
import org.junit.Rule;
import org.junit.Test;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;

public class OpenSearchAdapterTest {

  @Rule
  public TemporaryLogStoreAndSearcherRule logStoreAndSearcherRule =
      new TemporaryLogStoreAndSearcherRule(false);

  private final OpenSearchAdapter openSearchAdapter = new OpenSearchAdapter(Map.of());

  public OpenSearchAdapterTest() throws IOException {}

  @Test(expected = IllegalArgumentException.class)
  public void safelyHandlesUnknownAggregations() throws IOException {
    AggBuilder unknownAgg =
        new AggBuilderBase("foo") {
          @Override
          public String getType() {
            return "unknown";
          }
        };

    openSearchAdapter.buildAggregatorUsingContext(
        unknownAgg, logStoreAndSearcherRule.logStore.getSearcherManager().acquire());
  }

  @Test
  public void collectorManagerCorrectlyReducesListOfCollectors() throws IOException {
    AvgAggBuilder avgAggBuilder1 =
        new AvgAggBuilder("foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "2");
    AvgAggBuilder avgAggBuilder2 =
        new AvgAggBuilder("bar", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "2");
    CollectorManager<Aggregator, InternalAggregation> collectorManager1 =
        openSearchAdapter.getCollectorManager(
            avgAggBuilder1, logStoreAndSearcherRule.logStore.getSearcherManager().acquire());
    CollectorManager<Aggregator, InternalAggregation> collectorManager2 =
        openSearchAdapter.getCollectorManager(
            avgAggBuilder2, logStoreAndSearcherRule.logStore.getSearcherManager().acquire());

    Aggregator collector1 = collectorManager1.newCollector();
    Aggregator collector2 = collectorManager2.newCollector();

    InternalAvg reduced = (InternalAvg) collectorManager1.reduce(List.of(collector1, collector2));

    assertThat(reduced.getName()).isEqualTo("foo");
    assertThat(reduced.getType()).isEqualTo("avg");
    assertThat(reduced.getValue()).isEqualTo(Double.valueOf("NaN"));

    // todo - we don't have access to the package local methods for extra asserts - use reflection?
  }

  @Test
  public void canBuildValidAvgAggregator() throws IOException {
    AvgAggBuilder avgAggBuilder =
        new AvgAggBuilder("foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1");
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            avgAggBuilder, logStoreAndSearcherRule.logStore.getSearcherManager().acquire());

    try (Aggregator avgAggregator = collectorManager.newCollector()) {
      InternalAvg internalAvg = (InternalAvg) avgAggregator.buildTopLevel();

      assertThat(internalAvg.getName()).isEqualTo("foo");

      // todo - we don't have access to the package local methods for extra asserts - use
      // reflection?
    }
  }

  @Test
  public void canBuildValidUniqueCountAggregation() throws IOException {
    UniqueCountAggBuilder uniqueCountAggBuilder =
        new UniqueCountAggBuilder("foo", "service_name", "1", 0L);
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            uniqueCountAggBuilder, logStoreAndSearcherRule.logStore.getSearcherManager().acquire());

    try (Aggregator uniqueCountAggregator = collectorManager.newCollector()) {
      InternalCardinality internalUniqueCount =
          (InternalCardinality) uniqueCountAggregator.buildTopLevel();

      assertThat(internalUniqueCount.getName()).isEqualTo("foo");
      assertThat(internalUniqueCount.getValue()).isEqualTo(0);

      // todo - we don't have access to the package local methods for extra asserts - use
      // reflection?
    }
  }

  @Test
  public void canBuildValidDateHistogram() throws IOException {
    DateHistogramAggBuilder dateHistogramAggBuilder =
        new DateHistogramAggBuilder(
            "foo",
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            "5s",
            "2s",
            100,
            "epoch_ms",
            Map.of(),
            List.of());
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            dateHistogramAggBuilder,
            logStoreAndSearcherRule.logStore.getSearcherManager().acquire());

    try (Aggregator dateHistogramAggregator = collectorManager.newCollector()) {
      InternalDateHistogram internalDateHistogram =
          (InternalDateHistogram) dateHistogramAggregator.buildTopLevel();

      assertThat(internalDateHistogram.getName()).isEqualTo("foo");

      // todo - we don't have access to the package local methods for extra asserts - use
      // reflection?
    }
  }

  @Test
  public void canBuildValidHistogram() throws IOException {
    HistogramAggBuilder histogramAggBuilder =
        new HistogramAggBuilder(
            "foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1000", 1, List.of());
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            histogramAggBuilder, logStoreAndSearcherRule.logStore.getSearcherManager().acquire());

    try (Aggregator histogramAggregator = collectorManager.newCollector()) {
      InternalHistogram internalDateHistogram =
          (InternalHistogram) histogramAggregator.buildTopLevel();
      assertThat(internalDateHistogram.getName()).isEqualTo("foo");
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void handlesDateHistogramExtendedBoundsMinDocEdgeCases() throws IOException {
    // when using minDocCount the extended bounds must be set
    DateHistogramAggBuilder dateHistogramAggBuilder =
        new DateHistogramAggBuilder(
            "foo",
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            "5s",
            "2s",
            0,
            "epoch_ms",
            Map.of(),
            List.of());
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            dateHistogramAggBuilder,
            logStoreAndSearcherRule.logStore.getSearcherManager().acquire());

    try (Aggregator dateHistogramExtendedBounds = collectorManager.newCollector()) {
      InternalHistogram internalDateHistogram =
          (InternalHistogram) dateHistogramExtendedBounds.buildTopLevel();
      assertThat(internalDateHistogram.getName()).isEqualTo("foo");
    }
  }
}
