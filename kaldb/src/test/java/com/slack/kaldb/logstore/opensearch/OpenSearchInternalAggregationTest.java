package com.slack.kaldb.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.HistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.PercentilesAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.TermsAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.UniqueCountAggBuilder;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;

public class OpenSearchInternalAggregationTest {

  @Rule
  public TemporaryLogStoreAndSearcherRule logStoreAndSearcherRule =
      new TemporaryLogStoreAndSearcherRule(false);

  private final OpenSearchAdapter openSearchAdapter = new OpenSearchAdapter(Map.of(), false);

  public OpenSearchInternalAggregationTest() throws IOException {}

  @After
  public void tearDown() throws Exception {
    openSearchAdapter.close();
  }

  @Test
  public void canSerializeDeserializeInternalDateHistogramAggregation() throws IOException {
    AvgAggBuilder avgAggBuilder =
        new AvgAggBuilder("foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "3");
    DateHistogramAggBuilder dateHistogramAggBuilder =
        new DateHistogramAggBuilder(
            "foo", "epoch_ms", "10s", "5s", 10, "epoch_ms", Map.of(), List.of(avgAggBuilder));
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            dateHistogramAggBuilder,
            logStoreAndSearcherRule.logStore.getSearcherManager().acquire());
    InternalAggregation internalAggregation1 =
        collectorManager.reduce(Collections.singleton(collectorManager.newCollector()));

    byte[] serialize = OpenSearchInternalAggregation.toByteArray(internalAggregation1);
    InternalAggregation internalAggregation2 =
        OpenSearchInternalAggregation.fromByteArray(serialize);

    // todo - this is pending a PR to OpenSearch to address specific to histograms
    // https://github.com/opensearch-project/OpenSearch/pull/6357
    // this is because DocValueFormat.DateTime in OpenSearch does not implement a proper equals
    // method
    // As such the DocValueFormat.parser are never equal to each other
    assertThat(internalAggregation1.toString()).isEqualTo(internalAggregation2.toString());
  }

  @Test
  public void canSerializeDeserializeInternalHistogramAggregation() throws IOException {
    AvgAggBuilder avgAggBuilder =
        new AvgAggBuilder("foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "3");
    HistogramAggBuilder histogramAggBuilder =
        new HistogramAggBuilder("foo", "duration_ms", "1000", 1, List.of(avgAggBuilder));
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            histogramAggBuilder, logStoreAndSearcherRule.logStore.getSearcherManager().acquire());
    InternalAggregation internalAggregation1 =
        collectorManager.reduce(Collections.singleton(collectorManager.newCollector()));

    byte[] serialize = OpenSearchInternalAggregation.toByteArray(internalAggregation1);
    InternalAggregation internalAggregation2 =
        OpenSearchInternalAggregation.fromByteArray(serialize);

    // todo - this is pending a PR to OpenSearch to address specific to histograms
    // https://github.com/opensearch-project/OpenSearch/pull/6357
    // this is because DocValueFormat.DateTime in OpenSearch does not implement a proper equals
    // method
    // As such the DocValueFormat.parser are never equal to each other
    assertThat(internalAggregation1.toString()).isEqualTo(internalAggregation2.toString());
  }

  @Test
  public void canSerializeDeserializeInternalAggregationTerms() throws IOException {
    TermsAggBuilder termsAggBuilder =
        new TermsAggBuilder("1", List.of(), "service_name", "2", 10, 0, Map.of("_count", "asc"));
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            termsAggBuilder, logStoreAndSearcherRule.logStore.getSearcherManager().acquire());
    InternalAggregation internalAggregation1 =
        collectorManager.reduce(Collections.singleton(collectorManager.newCollector()));

    byte[] serialize = OpenSearchInternalAggregation.toByteArray(internalAggregation1);
    InternalAggregation internalAggregation2 =
        OpenSearchInternalAggregation.fromByteArray(serialize);

    assertThat(internalAggregation1).isEqualTo(internalAggregation2);
  }

  @Test
  public void canSerializeDeserializeInternalPercentiles() throws IOException {
    PercentilesAggBuilder percentilesAggBuilder =
        new PercentilesAggBuilder("1", "service_name", null, List.of(95D, 99D));
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            percentilesAggBuilder, logStoreAndSearcherRule.logStore.getSearcherManager().acquire());
    InternalAggregation internalAggregation1 =
        collectorManager.reduce(Collections.singleton(collectorManager.newCollector()));

    byte[] serialize = OpenSearchInternalAggregation.toByteArray(internalAggregation1);
    InternalAggregation internalAggregation2 =
        OpenSearchInternalAggregation.fromByteArray(serialize);
    assertThat(internalAggregation1).isEqualTo(internalAggregation2);
  }

  @Test
  public void canSerializeDeserializeInternalUniqueCount() throws IOException {
    IndexSearcher indexSearcher = logStoreAndSearcherRule.logStore.getSearcherManager().acquire();

    UniqueCountAggBuilder uniqueCountAggBuilder1 =
        new UniqueCountAggBuilder("1", "service_name", "3", null);
    CollectorManager<Aggregator, InternalAggregation> collectorManager1 =
        openSearchAdapter.getCollectorManager(uniqueCountAggBuilder1, indexSearcher);
    InternalAggregation internalAggregation1 =
        collectorManager1.reduce(Collections.singleton(collectorManager1.newCollector()));
    byte[] serialize = OpenSearchInternalAggregation.toByteArray(internalAggregation1);
    InternalAggregation internalAggregation2 =
        OpenSearchInternalAggregation.fromByteArray(serialize);

    assertThat(internalAggregation1.toString()).isEqualTo(internalAggregation2.toString());

    UniqueCountAggBuilder uniqueCountAggBuilder3 =
        new UniqueCountAggBuilder("1", "service_name", "3", 3L);
    CollectorManager<Aggregator, InternalAggregation> collectorManager3 =
        openSearchAdapter.getCollectorManager(uniqueCountAggBuilder3, indexSearcher);
    InternalAggregation internalAggregation3 =
        collectorManager3.reduce(Collections.singleton(collectorManager3.newCollector()));
    byte[] serialize2 = OpenSearchInternalAggregation.toByteArray(internalAggregation3);
    InternalAggregation internalAggregation4 =
        OpenSearchInternalAggregation.fromByteArray(serialize2);

    assertThat(internalAggregation3.toString()).isEqualTo(internalAggregation4.toString());
  }
}
