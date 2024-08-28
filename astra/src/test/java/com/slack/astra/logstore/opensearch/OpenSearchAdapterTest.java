package com.slack.astra.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import com.google.common.collect.ImmutableMap;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.aggregations.AggBuilder;
import com.slack.astra.logstore.search.aggregations.AggBuilderBase;
import com.slack.astra.logstore.search.aggregations.AutoDateHistogramAggBuilder;
import com.slack.astra.logstore.search.aggregations.AvgAggBuilder;
import com.slack.astra.logstore.search.aggregations.CumulativeSumAggBuilder;
import com.slack.astra.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.astra.logstore.search.aggregations.DerivativeAggBuilder;
import com.slack.astra.logstore.search.aggregations.ExtendedStatsAggBuilder;
import com.slack.astra.logstore.search.aggregations.HistogramAggBuilder;
import com.slack.astra.logstore.search.aggregations.MaxAggBuilder;
import com.slack.astra.logstore.search.aggregations.MinAggBuilder;
import com.slack.astra.logstore.search.aggregations.MovingAvgAggBuilder;
import com.slack.astra.logstore.search.aggregations.MovingFunctionAggBuilder;
import com.slack.astra.logstore.search.aggregations.SumAggBuilder;
import com.slack.astra.logstore.search.aggregations.UniqueCountAggBuilder;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.pipeline.CumulativeSumPipelineAggregator;
import org.opensearch.search.aggregations.pipeline.DerivativePipelineAggregator;
import org.opensearch.search.aggregations.pipeline.MovAvgPipelineAggregator;
import org.opensearch.search.aggregations.pipeline.MovFnPipelineAggregator;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;

public class OpenSearchAdapterTest {

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension logStoreAndSearcherRule =
      new TemporaryLogStoreAndSearcherExtension(false);

  private final ImmutableMap.Builder<String, LuceneFieldDef> fieldDefBuilder;
  private final OpenSearchAdapter openSearchAdapter;

  public OpenSearchAdapterTest() throws IOException {
    fieldDefBuilder = ImmutableMap.builder();
    fieldDefBuilder.put(
        LogMessage.SystemField.ID.fieldName,
        new LuceneFieldDef(
            LogMessage.SystemField.ID.fieldName, FieldType.ID.name, false, true, true));
    fieldDefBuilder.put(
        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
        new LuceneFieldDef(
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            FieldType.LONG.name,
            false,
            true,
            true));
    openSearchAdapter = new OpenSearchAdapter(fieldDefBuilder.build());
    // We need to reload the schema so that query optimizations take into account the schema
    openSearchAdapter.reloadSchema();
  }

  @Test
  public void safelyHandlesUnknownAggregations() throws IOException {
    AggBuilder unknownAgg =
        new AggBuilderBase("foo") {
          @Override
          public String getType() {
            return "unknown";
          }
        };

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                openSearchAdapter.buildAggregatorUsingContext(
                    unknownAgg,
                    logStoreAndSearcherRule.logStore.getSearcherManager().acquire(),
                    null));
  }

  @Test
  public void collectorManagerCorrectlyReducesListOfCollectors() throws IOException {
    AvgAggBuilder avgAggBuilder1 =
        new AvgAggBuilder("foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "2", null);
    AvgAggBuilder avgAggBuilder2 =
        new AvgAggBuilder("bar", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "2", null);
    CollectorManager<Aggregator, InternalAggregation> collectorManager1 =
        openSearchAdapter.getCollectorManager(
            avgAggBuilder1, logStoreAndSearcherRule.logStore.getSearcherManager().acquire(), null);
    CollectorManager<Aggregator, InternalAggregation> collectorManager2 =
        openSearchAdapter.getCollectorManager(
            avgAggBuilder2, logStoreAndSearcherRule.logStore.getSearcherManager().acquire(), null);

    Aggregator collector1 = collectorManager1.newCollector();
    Aggregator collector2 = collectorManager2.newCollector();

    InternalAvg reduced = (InternalAvg) collectorManager1.reduce(List.of(collector1, collector2));

    assertThat(reduced.getName()).isEqualTo("foo");
    assertThat(reduced.getType()).isEqualTo("avg");
    assertThat(reduced.getValue()).isEqualTo(Double.valueOf("NaN"));

    // todo - we don't have access to the package local methods for extra asserts - use reflection?
  }

  @Test
  public void canBuildValidMinAggregator() throws IOException {
    MinAggBuilder minAggBuilder =
        new MinAggBuilder("foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1", null);
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            minAggBuilder, logStoreAndSearcherRule.logStore.getSearcherManager().acquire(), null);

    try (Aggregator minAggregator = collectorManager.newCollector()) {
      InternalMin internalMin = (InternalMin) minAggregator.buildTopLevel();

      assertThat(internalMin.getName()).isEqualTo("foo");

      // TODO - we don't have access to the package local methods for extra asserts - use
      // reflection?
    }
  }

  @Test
  public void canBuildValidMaxAggregator() throws IOException {
    MaxAggBuilder maxAggBuilder =
        new MaxAggBuilder(
            "foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1", "return 8;");
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            maxAggBuilder, logStoreAndSearcherRule.logStore.getSearcherManager().acquire(), null);

    try (Aggregator maxAggregator = collectorManager.newCollector()) {
      InternalMax internalMax = (InternalMax) maxAggregator.buildTopLevel();

      assertThat(internalMax.getName()).isEqualTo("foo");

      // TODO - we don't have access to the package local methods for extra asserts - use
      // reflection?
    }
  }

  @Test
  public void canBuildValidSumAggregator() throws IOException {
    SumAggBuilder sumAggBuilder =
        new SumAggBuilder("foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1", "");
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            sumAggBuilder, logStoreAndSearcherRule.logStore.getSearcherManager().acquire(), null);

    try (Aggregator avgAggregator = collectorManager.newCollector()) {
      InternalSum internalSum = (InternalSum) avgAggregator.buildTopLevel();

      assertThat(internalSum.getName()).isEqualTo("foo");

      // todo - we don't have access to the package local methods for extra asserts - use
      // reflection?
    }
  }

  @Test
  public void canBuildValidAvgAggregator() throws IOException {
    AvgAggBuilder avgAggBuilder =
        new AvgAggBuilder("foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1", "");
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            avgAggBuilder, logStoreAndSearcherRule.logStore.getSearcherManager().acquire(), null);

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
            uniqueCountAggBuilder,
            logStoreAndSearcherRule.logStore.getSearcherManager().acquire(),
            null);

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
  public void canBuildValidExtendedStatsAggregator() throws IOException {
    ExtendedStatsAggBuilder extendedStatsAggBuilder =
        new ExtendedStatsAggBuilder(
            "foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1", "", 2D);
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            extendedStatsAggBuilder,
            logStoreAndSearcherRule.logStore.getSearcherManager().acquire(),
            null);

    try (Aggregator avgAggregator = collectorManager.newCollector()) {
      InternalExtendedStats internalExtendedStats =
          (InternalExtendedStats) avgAggregator.buildTopLevel();

      assertThat(internalExtendedStats.getName()).isEqualTo("foo");
      assertThat(internalExtendedStats.getSigma()).isEqualTo(2);

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
            null,
            100,
            "epoch_ms",
            Map.of(),
            List.of());
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            dateHistogramAggBuilder,
            logStoreAndSearcherRule.logStore.getSearcherManager().acquire(),
            null);

    try (Aggregator dateHistogramAggregator = collectorManager.newCollector()) {
      InternalDateHistogram internalDateHistogram =
          (InternalDateHistogram) dateHistogramAggregator.buildTopLevel();

      assertThat(internalDateHistogram.getName()).isEqualTo("foo");

      // todo - we don't have access to the package local methods for extra asserts - use
      // reflection?
    }
  }

  @Test
  public void canBuildValidAutoDateHistogram() throws IOException {
    AutoDateHistogramAggBuilder autoDateHistogramAggBuilder =
        new AutoDateHistogramAggBuilder(
            "foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, null, null, List.of());

    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            autoDateHistogramAggBuilder,
            logStoreAndSearcherRule.logStore.getSearcherManager().acquire(),
            null);

    try (Aggregator autoDateHistogramAggregator = collectorManager.newCollector()) {
      InternalAutoDateHistogram internalDateHistogram =
          (InternalAutoDateHistogram) autoDateHistogramAggregator.buildTopLevel();

      assertThat(internalDateHistogram.getName()).isEqualTo("foo");

      // todo - we don't have access to the package local methods for extra asserts - use
      // reflection?
    }
  }

  @Test
  public void canBuildValidCumulativeSumPipelineAggregator() {
    DateHistogramAggBuilder dateHistogramWithCumulativeSum =
        new DateHistogramAggBuilder(
            "foo",
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            "5s",
            "2s",
            null,
            100,
            "epoch_ms",
            Map.of(),
            List.of(new CumulativeSumAggBuilder("bar", "_count", "##0.#####E0")));

    AbstractAggregationBuilder builder =
        OpenSearchAdapter.getAggregationBuilder(dateHistogramWithCumulativeSum);
    PipelineAggregator.PipelineTree pipelineTree = builder.buildPipelineTree();

    assertThat(pipelineTree.aggregators().size()).isEqualTo(1);
    CumulativeSumPipelineAggregator cumulativeSumPipelineAggregator =
        (CumulativeSumPipelineAggregator) pipelineTree.aggregators().get(0);
    assertThat(cumulativeSumPipelineAggregator.bucketsPaths()).isEqualTo(new String[] {"_count"});
    assertThat(cumulativeSumPipelineAggregator.name()).isEqualTo("bar");

    // TODO - we don't have access to the package local methods for extra asserts - use
    //  reflection?
  }

  @Test
  public void canBuildValidMovingFunctionPipelineAggregator() {
    DateHistogramAggBuilder dateHistogramWithMovingFn =
        new DateHistogramAggBuilder(
            "foo",
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            "5s",
            "2s",
            null,
            100,
            "epoch_ms",
            Map.of(),
            List.of(new MovingFunctionAggBuilder("bar", "_count", "return 8;", 10, null)));

    AbstractAggregationBuilder builder =
        OpenSearchAdapter.getAggregationBuilder(dateHistogramWithMovingFn);
    PipelineAggregator.PipelineTree pipelineTree = builder.buildPipelineTree();

    assertThat(pipelineTree.aggregators().size()).isEqualTo(1);
    MovFnPipelineAggregator movingFnAggregator =
        (MovFnPipelineAggregator) pipelineTree.aggregators().get(0);
    assertThat(movingFnAggregator.bucketsPaths()).isEqualTo(new String[] {"_count"});
    assertThat(movingFnAggregator.name()).isEqualTo("bar");

    // TODO - we don't have access to the package local methods for extra asserts - use
    //  reflection?
  }

  @Test
  public void canBuildValidMovingAveragePipelineAggregator() {
    DateHistogramAggBuilder dateHistogramWithDerivative =
        new DateHistogramAggBuilder(
            "foo",
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            "5s",
            "2s",
            null,
            100,
            "epoch_ms",
            Map.of(),
            List.of(new MovingAvgAggBuilder("bar", "_count", "linear", 5, 2)));

    AbstractAggregationBuilder builder =
        OpenSearchAdapter.getAggregationBuilder(dateHistogramWithDerivative);
    PipelineAggregator.PipelineTree pipelineTree = builder.buildPipelineTree();

    assertThat(pipelineTree.aggregators().size()).isEqualTo(1);
    MovAvgPipelineAggregator movAvgPipelineAggregator =
        (MovAvgPipelineAggregator) pipelineTree.aggregators().get(0);
    assertThat(movAvgPipelineAggregator.bucketsPaths()).isEqualTo(new String[] {"_count"});
    assertThat(movAvgPipelineAggregator.name()).isEqualTo("bar");

    // TODO - we don't have access to the package local methods for extra asserts - use
    //  reflection?
  }

  @Test
  public void canBuildValidDerivativePipelineAggregator() {
    DateHistogramAggBuilder dateHistogramWithDerivative =
        new DateHistogramAggBuilder(
            "foo",
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            "5s",
            "2s",
            null,
            100,
            "epoch_ms",
            Map.of(),
            List.of(new DerivativeAggBuilder("bar", "_count", null)));

    AbstractAggregationBuilder builder =
        OpenSearchAdapter.getAggregationBuilder(dateHistogramWithDerivative);
    PipelineAggregator.PipelineTree pipelineTree = builder.buildPipelineTree();

    assertThat(pipelineTree.aggregators().size()).isEqualTo(1);
    DerivativePipelineAggregator derivativePipelineAggregator =
        (DerivativePipelineAggregator) pipelineTree.aggregators().get(0);
    assertThat(derivativePipelineAggregator.bucketsPaths()).isEqualTo(new String[] {"_count"});
    assertThat(derivativePipelineAggregator.name()).isEqualTo("bar");

    // TODO - we don't have access to the package local methods for extra asserts - use
    //  reflection?
  }

  @Test
  public void canBuildValidHistogram() throws IOException {
    HistogramAggBuilder histogramAggBuilder =
        new HistogramAggBuilder(
            "foo", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1000", 1, List.of());
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            histogramAggBuilder,
            logStoreAndSearcherRule.logStore.getSearcherManager().acquire(),
            null);

    try (Aggregator histogramAggregator = collectorManager.newCollector()) {
      InternalHistogram internalDateHistogram =
          (InternalHistogram) histogramAggregator.buildTopLevel();
      assertThat(internalDateHistogram.getName()).isEqualTo("foo");
    }
  }

  @Test
  public void handlesDateHistogramExtendedBoundsMinDocEdgeCases() throws IOException {
    // when using minDocCount the extended bounds must be set
    DateHistogramAggBuilder dateHistogramAggBuilder =
        new DateHistogramAggBuilder(
            "foo",
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            "5s",
            "2s",
            null,
            0,
            "epoch_ms",
            Map.of(),
            List.of());
    CollectorManager<Aggregator, InternalAggregation> collectorManager =
        openSearchAdapter.getCollectorManager(
            dateHistogramAggBuilder,
            logStoreAndSearcherRule.logStore.getSearcherManager().acquire(),
            null);

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> collectorManager.newCollector());
  }

  @Test
  public void shouldProduceQueryFromQueryBuilder() throws Exception {
    BoolQueryBuilder boolQueryBuilder =
        new BoolQueryBuilder().filter(new RangeQueryBuilder("_timesinceepoch").gte(1).lte(100));
    IndexSearcher indexSearcher = logStoreAndSearcherRule.logStore.getSearcherManager().acquire();

    // We need to recreate the OpenSearchAdapter object here to get the feature flag set to true.
    // Once this feature flag is removed, we can once again use the class-level object
    System.setProperty("astra.query.useOpenSearchParsing", "true");
    OpenSearchAdapter openSearchAdapterWithFeatureFlagEnabled =
        new OpenSearchAdapter(fieldDefBuilder.build());
    openSearchAdapterWithFeatureFlagEnabled.reloadSchema();
    System.setProperty("astra.query.useOpenSearchParsing", "false");

    Query rangeQuery =
        openSearchAdapterWithFeatureFlagEnabled.buildQuery(
            indexSearcher, boolQueryBuilder);
    assertThat(rangeQuery).isNotNull();
    assertThat(rangeQuery.toString()).isEqualTo("#_timesinceepoch:[1 TO 100]");
  }

  @Test
  public void shouldParseIdFieldSearch() throws Exception {
    String idField = "_id";
    String idValue = "1";
    IndexSearcher indexSearcher = logStoreAndSearcherRule.logStore.getSearcherManager().acquire();
    Query idQuery =
        openSearchAdapter.buildQuery(
            indexSearcher, new QueryStringQueryBuilder(String.format("%s:%s", idField, idValue)));
    BytesRef queryStrBytes = new BytesRef(Uid.encodeId("1").bytes);
    // idQuery.toString="#_id:([fe 1f])"
    // queryStrBytes.toString="[fe 1f]"
    assertThat(idQuery.toString()).contains(queryStrBytes.toString());
  }

  @Test
  public void shouldExcludeDateFilterWhenNullTimestamps() throws Exception {
    IndexSearcher indexSearcher = logStoreAndSearcherRule.logStore.getSearcherManager().acquire();
    Query nullBothTimestamps =
        openSearchAdapter.buildQuery(indexSearcher, new QueryStringQueryBuilder(""));
    // null for both timestamps with no query string should be optimized into a matchall
    assertThat(nullBothTimestamps).isInstanceOf(MatchAllDocsQuery.class);

    Query nullStartTimestamp =
        openSearchAdapter.buildQuery("foo", "a", null, 100L, indexSearcher, null);
    assertThat(nullStartTimestamp).isInstanceOf(BooleanQuery.class);

    Optional<IndexSortSortedNumericDocValuesRangeQuery> filterNullStartQuery =
        ((BooleanQuery) nullStartTimestamp)
            .clauses().stream()
                .filter(
                    booleanClause ->
                        booleanClause.getQuery()
                            instanceof IndexSortSortedNumericDocValuesRangeQuery)
                .map(
                    booleanClause ->
                        (IndexSortSortedNumericDocValuesRangeQuery) booleanClause.getQuery())
                .findFirst();
    assertThat(filterNullStartQuery).isPresent();
    // a null start and provided end should result in an optimized range query of min long to the
    // end value
    assertThat(filterNullStartQuery.get().toString()).contains(String.valueOf(Long.MIN_VALUE));
    assertThat(filterNullStartQuery.get().toString()).contains(String.valueOf(100L));

    Query nullEndTimestamp =
        openSearchAdapter.buildQuery("foo", "", 100L, null, indexSearcher, null);
    Optional<IndexSortSortedNumericDocValuesRangeQuery> filterNullEndQuery =
        ((BooleanQuery) nullEndTimestamp)
            .clauses().stream()
                .filter(
                    booleanClause ->
                        booleanClause.getQuery()
                            instanceof IndexSortSortedNumericDocValuesRangeQuery)
                .map(
                    booleanClause ->
                        (IndexSortSortedNumericDocValuesRangeQuery) booleanClause.getQuery())
                .findFirst();
    assertThat(filterNullEndQuery).isPresent();
    // a null end and provided start should result in an optimized range query of start value to max
    // long
    assertThat(filterNullEndQuery.get().toString()).contains(String.valueOf(100L));
    assertThat(filterNullEndQuery.get().toString()).contains(String.valueOf(Long.MAX_VALUE));
  }
}
