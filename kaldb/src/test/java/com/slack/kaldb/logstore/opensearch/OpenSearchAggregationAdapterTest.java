package com.slack.kaldb.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.logstore.search.aggregations.AggBuilder;
import com.slack.kaldb.logstore.search.aggregations.AggBuilderBase;
import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.CollectorManager;
import org.junit.Test;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.metrics.InternalAvg;

public class OpenSearchAggregationAdapterTest {

  @Test
  public void canSerializeDeserializeInternalAggregation() throws IOException {
    OpenSearchAggregationAdapter openSearchAggregationAdapter =
        new OpenSearchAggregationAdapter(Map.of());

    AvgAggBuilder avgAggBuilder = new AvgAggBuilder("foo", "@timestamp");
    DateHistogramAggBuilder dateHistogramAggBuilder =
        new DateHistogramAggBuilder(
            "foo", "epoch_ms", "10s", "5s", 10, "epoch_ms", Map.of(), List.of(avgAggBuilder));
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAggregationAdapter.getCollectorManager(dateHistogramAggBuilder);
    InternalAggregation internalAggregation1 =
        collectorManager.reduce(Collections.singleton(collectorManager.newCollector()));

    byte[] serialize = OpenSearchAggregationAdapter.toByteArray(internalAggregation1);
    InternalAggregation internalAggregation2 =
        OpenSearchAggregationAdapter.fromByteArray(serialize);

    // todo - this is pending a PR to OpenSearch to address
    // https://github.com/opensearch-project/OpenSearch/pull/6357
    // this is because DocValueFormat.DateTime in OpenSearch does not implement a proper equals
    // method
    // As such the DocValueFormat.parser are never equal to each other
    assertThat(internalAggregation1.toString()).isEqualTo(internalAggregation2.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void safelyHandlesUnknownAggregations() throws IOException {
    OpenSearchAggregationAdapter openSearchAggregationAdapter =
        new OpenSearchAggregationAdapter(Map.of());
    AggBuilder unknownAgg =
        new AggBuilderBase("foo") {
          @Override
          public String getType() {
            return "unknown";
          }
        };

    openSearchAggregationAdapter.buildAggregatorUsingContext(unknownAgg);
  }

  @Test
  public void collectorManagerCorrectlyReducesListOfCollectors() throws IOException {
    OpenSearchAggregationAdapter openSearchAggregationAdapter =
        new OpenSearchAggregationAdapter(Map.of());

    AvgAggBuilder avgAggBuilder1 = new AvgAggBuilder("foo", "@timestamp");
    AvgAggBuilder avgAggBuilder2 = new AvgAggBuilder("bar", "@timestamp");
    CollectorManager<Aggregator, InternalAggregation> collectorManager1 =
        openSearchAggregationAdapter.getCollectorManager(avgAggBuilder1);
    CollectorManager<Aggregator, InternalAggregation> collectorManager2 =
        openSearchAggregationAdapter.getCollectorManager(avgAggBuilder2);

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
    OpenSearchAggregationAdapter openSearchAggregationAdapter =
        new OpenSearchAggregationAdapter(Map.of());
    AvgAggBuilder avgAggBuilder = new AvgAggBuilder("foo", "@timestamp");
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAggregationAdapter.getCollectorManager(avgAggBuilder);

    Aggregator avgAggregator = collectorManager.newCollector();
    InternalAvg internalAvg = (InternalAvg) avgAggregator.buildTopLevel();

    assertThat(internalAvg.getName()).isEqualTo("foo");

    // todo - we don't have access to the package local methods for extra asserts - use reflection?
  }

  @Test
  public void canBuildValidDateHistogram() throws IOException {
    OpenSearchAggregationAdapter openSearchAggregationAdapter =
        new OpenSearchAggregationAdapter(Map.of());
    DateHistogramAggBuilder dateHistogramAggBuilder =
        new DateHistogramAggBuilder(
            "foo", "@timestamp", "5s", "2s", 100, "epoch_ms", Map.of(), List.of());
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAggregationAdapter.getCollectorManager(dateHistogramAggBuilder);

    Aggregator dateHistogramAggregator = collectorManager.newCollector();
    InternalDateHistogram internalDateHistogram =
        (InternalDateHistogram) dateHistogramAggregator.buildTopLevel();

    assertThat(internalDateHistogram.getName()).isEqualTo("foo");

    // todo - we don't have access to the package local methods for extra asserts - use reflection?
  }

  @Test(expected = IllegalArgumentException.class)
  public void handlesDateHistogramExtendedBoundsMinDocEdgeCases() throws IOException {
    OpenSearchAggregationAdapter openSearchAggregationAdapter =
        new OpenSearchAggregationAdapter(Map.of());

    // when using minDocCount the extended bounds must be set
    DateHistogramAggBuilder dateHistogramAggBuilder =
        new DateHistogramAggBuilder(
            "foo", "@timestamp", "5s", "2s", 0, "epoch_ms", Map.of(), List.of());
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAggregationAdapter.getCollectorManager(dateHistogramAggBuilder);
    collectorManager.newCollector();
  }
}
