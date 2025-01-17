package com.slack.astra.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension;
import com.slack.astra.util.QueryBuilderUtil;
import java.io.IOException;
import java.util.List;
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
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;

public class OpenSearchAdapterTest {

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension logStoreAndSearcherRule =
      new TemporaryLogStoreAndSearcherExtension(false);

  private final OpenSearchAdapter openSearchAdapter;

  public OpenSearchAdapterTest() throws IOException {
    ImmutableMap.Builder<String, LuceneFieldDef> fieldDefBuilder = ImmutableMap.builder();
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
  public void testOpenSearchCollectorManagerCorrectlyReducesListOfCollectors() throws IOException {
    AvgAggregationBuilder avgAggregationBuilder = new AvgAggregationBuilder("foo");
    avgAggregationBuilder.field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
    avgAggregationBuilder.missing("2");

    AvgAggregationBuilder avgAggregationBuilder2 = new AvgAggregationBuilder("bar");
    avgAggregationBuilder2.field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
    avgAggregationBuilder2.missing("2");

    AggregatorFactories.Builder aggregatorFactoriesBuilder = new AggregatorFactories.Builder();
    aggregatorFactoriesBuilder.addAggregator(avgAggregationBuilder);

    AggregatorFactories.Builder aggregatorFactoriesBuilder2 = new AggregatorFactories.Builder();
    aggregatorFactoriesBuilder2.addAggregator(avgAggregationBuilder2);

    CollectorManager<Aggregator, InternalAggregation> collectorManager1 =
        openSearchAdapter.getCollectorManager(
            aggregatorFactoriesBuilder,
            logStoreAndSearcherRule
                .logStore
                .getAstraSearcherManager()
                .getLuceneSearcherManager()
                .acquire(),
            null);
    CollectorManager<Aggregator, InternalAggregation> collectorManager2 =
        openSearchAdapter.getCollectorManager(
            aggregatorFactoriesBuilder2,
            logStoreAndSearcherRule
                .logStore
                .getAstraSearcherManager()
                .getLuceneSearcherManager()
                .acquire(),
            null);

    Aggregator collector1 = collectorManager1.newCollector();
    Aggregator collector2 = collectorManager2.newCollector();

    InternalAvg reduced = (InternalAvg) collectorManager1.reduce(List.of(collector1, collector2));

    assertThat(reduced.getName()).isEqualTo("foo");
    assertThat(reduced.getType()).isEqualTo("avg");
    assertThat(reduced.getValue()).isEqualTo(Double.valueOf("NaN"));

    // todo - we don't have access to the package local methods for extra asserts - use reflection?
  }

  @Test
  public void shouldProduceQueryFromQueryBuilder() throws Exception {
    BoolQueryBuilder boolQueryBuilder =
        new BoolQueryBuilder().filter(new RangeQueryBuilder("_timesinceepoch").gte(1).lte(100));
    IndexSearcher indexSearcher =
        logStoreAndSearcherRule
            .logStore
            .getAstraSearcherManager()
            .getLuceneSearcherManager()
            .acquire();

    Query rangeQuery = openSearchAdapter.buildQuery(indexSearcher, boolQueryBuilder);
    assertThat(rangeQuery).isNotNull();
    assertThat(rangeQuery.toString()).isEqualTo("#_timesinceepoch:[1 TO 100]");
  }

  @Test
  public void shouldParseIdFieldSearch() throws Exception {
    String idField = "_id";
    String idValue = "1";
    IndexSearcher indexSearcher =
        logStoreAndSearcherRule
            .logStore
            .getAstraSearcherManager()
            .getLuceneSearcherManager()
            .acquire();
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
    IndexSearcher indexSearcher =
        logStoreAndSearcherRule
            .logStore
            .getAstraSearcherManager()
            .getLuceneSearcherManager()
            .acquire();
    Query nullBothTimestamps =
        openSearchAdapter.buildQuery(
            indexSearcher, QueryBuilderUtil.generateQueryBuilder("", null, null));
    // null for both timestamps with no query string should be optimized into a matchall
    assertThat(nullBothTimestamps).isInstanceOf(MatchAllDocsQuery.class);

    Query nullStartTimestamp =
        openSearchAdapter.buildQuery(
            indexSearcher, QueryBuilderUtil.generateQueryBuilder("a", null, 100L));
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
        openSearchAdapter.buildQuery(
            indexSearcher, QueryBuilderUtil.generateQueryBuilder("", 100L, null));
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

  @Test
  public void shouldHandleFieldsStartingWithDot() throws IOException {
    // Define a schema with a field name starting with a dot
    ImmutableMap<String, LuceneFieldDef> chunkSchema =
        ImmutableMap.of(".ipv4", new LuceneFieldDef(".ipv4", FieldType.IP.name, false, true, true));

    // Create a new OpenSearchAdapter instance with this schema
    OpenSearchAdapter adapter = new OpenSearchAdapter(chunkSchema);

    // Run loadSchema and assert it passes (does not throw an exception)
    try {
      adapter.loadSchema(); // This should pass
    } catch (RuntimeException e) {
      // The test fails if the exception message contains "name cannot be empty string"
      assertThat(e.getMessage()).doesNotContain("name cannot be empty string");
      org.junit.jupiter.api.Assertions.fail("Unexpected exception thrown: " + e.getMessage());
    }
  }

  @Test
  public void shouldHandleFieldsWithDotInMiddle() throws IOException {
    // Define a schema with a field name starting with a dot
    ImmutableMap<String, LuceneFieldDef> chunkSchema =
        ImmutableMap.of(
            "something.ipv4",
            new LuceneFieldDef("something.ipv4", FieldType.IP.name, false, true, true));

    // Create a new OpenSearchAdapter instance with this schema
    OpenSearchAdapter adapter = new OpenSearchAdapter(chunkSchema);

    // Run loadSchema and assert it passes (does not throw an exception)
    try {
      adapter.loadSchema(); // This should pass
    } catch (RuntimeException e) {
      // The test fails if there is any exception
      org.junit.jupiter.api.Assertions.fail("Unexpected exception thrown: " + e.getMessage());
    }
  }
}
