package com.slack.astra.logstore.search;

import static com.slack.astra.logstore.search.SearchResultUtils.fromValueProto;
import static com.slack.astra.logstore.search.SearchResultUtils.toValueProto;
import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.proto.service.AstraSearch;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.IntervalQueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;

public class SearchResultUtilsTest {

  @Test
  public void shouldParseAggIntoAggregationFactoriesBuilder() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setAggregationJson(
                """
              {
                "2": {
                  "date_histogram": {
                    "interval": "10s",
                    "field": "_timesinceepoch",
                    "min_doc_count": "90000",
                    "extended_bounds": {
                      "min": 1676498801027,
                      "max": 1676500240688
                    },
                    "format": "epoch_millis",
                    "offset": "5s"
                  },
                  "aggs":{}
                }
              }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.aggregatorFactoriesBuilder).isNotNull();
    assertThat(output.aggregatorFactoriesBuilder.getAggregatorFactories()).hasSize(1);

    Collection<AggregationBuilder> aggregatorFactories =
        output.aggregatorFactoriesBuilder.getAggregatorFactories();
    AggregationBuilder firstAggregationBuilder = aggregatorFactories.iterator().next();
    assertThat(firstAggregationBuilder.getName()).isEqualTo("2");
    assertThat(firstAggregationBuilder).isInstanceOf(DateHistogramAggregationBuilder.class);

    DateHistogramAggregationBuilder dateHistogramAggregationBuilder =
        (DateHistogramAggregationBuilder) firstAggregationBuilder;
    assertThat(dateHistogramAggregationBuilder.field()).isEqualTo("_timesinceepoch");
    assertThat(dateHistogramAggregationBuilder.minDocCount()).isEqualTo(90000L);
    assertThat(dateHistogramAggregationBuilder.extendedBounds().getMin()).isEqualTo(1676498801027L);
    assertThat(dateHistogramAggregationBuilder.extendedBounds().getMax()).isEqualTo(1676500240688L);
    assertThat(dateHistogramAggregationBuilder.format()).isEqualTo("epoch_millis");
    assertThat(dateHistogramAggregationBuilder.offset()).isEqualTo(5000L);
  }

  @Test
  public void shouldAppendDefaultFieldToQueryStringQuery() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "query_string": {
                "analyze_wildcard": true,
                "query": "Message70"
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(QueryStringQueryBuilder.class);

    QueryStringQueryBuilder queryStringQueryBuilder = (QueryStringQueryBuilder) output.queryBuilder;
    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("Message70");
    assertThat(queryStringQueryBuilder.analyzeWildcard()).isTrue();
    assertThat(queryStringQueryBuilder.fields()).containsKeys("_all", "*");
    assertThat(queryStringQueryBuilder.fields().get("_all")).isEqualTo(1.0f);
    assertThat(queryStringQueryBuilder.fields().get("*")).isEqualTo(1.0f);
  }

  @Test
  public void shouldNotOverrideExistingDefaultFieldInQueryString() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "query_string": {
                "analyze_wildcard": true,
                "query": "Message70",
                "default_field": "custom_field"
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(QueryStringQueryBuilder.class);

    QueryStringQueryBuilder queryStringQueryBuilder = (QueryStringQueryBuilder) output.queryBuilder;
    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("Message70");
    assertThat(queryStringQueryBuilder.analyzeWildcard()).isTrue();
    assertThat(queryStringQueryBuilder.defaultField()).isEqualTo("custom_field");
    assertThat(queryStringQueryBuilder.fields()).isEmpty();
  }

  @Test
  public void shouldNotOverrideExistingFieldsInQueryString() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "query_string": {
                "analyze_wildcard": true,
                "query": "Message70",
                "fields": ["field1^2", "field2"]
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(QueryStringQueryBuilder.class);

    QueryStringQueryBuilder queryStringQueryBuilder = (QueryStringQueryBuilder) output.queryBuilder;
    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("Message70");
    assertThat(queryStringQueryBuilder.analyzeWildcard()).isTrue();
    assertThat(queryStringQueryBuilder.fields()).containsKeys("field1", "field2");
    assertThat(queryStringQueryBuilder.fields().get("field1")).isEqualTo(2.0f);
    assertThat(queryStringQueryBuilder.fields().get("field2")).isEqualTo(1.0f);
  }

  @Test
  public void shouldAppendDefaultFieldToNestedQueryStringInBoolQuery() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "bool": {
                "must": {
                  "query_string": {
                    "analyze_wildcard": true,
                    "query": "Message70"
                  }
                }
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);

    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
    assertThat(boolQueryBuilder.must().size()).isEqualTo(1);
    assertThat(boolQueryBuilder.must().getFirst()).isInstanceOf(QueryStringQueryBuilder.class);

    QueryStringQueryBuilder queryStringQueryBuilder =
        (QueryStringQueryBuilder) boolQueryBuilder.must().getFirst();
    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("Message70");
    assertThat(queryStringQueryBuilder.analyzeWildcard()).isTrue();
    assertThat(queryStringQueryBuilder.fields()).containsKeys("_all", "*");
    assertThat(queryStringQueryBuilder.fields().get("_all")).isEqualTo(1.0f);
    assertThat(queryStringQueryBuilder.fields().get("*")).isEqualTo(1.0f);
  }

  @Test
  public void shouldParseBasicMustNotQueryIntoQueryBuilder() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "bool": {
                "must_not": {
                  "query_string": {
                    "query": "this is a test"
                  }
                }
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);

    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
    assertThat(boolQueryBuilder.mustNot().size()).isEqualTo(1);
    assertThat(boolQueryBuilder.mustNot().getFirst()).isInstanceOf(QueryStringQueryBuilder.class);

    QueryStringQueryBuilder queryStringQueryBuilder =
        (QueryStringQueryBuilder) boolQueryBuilder.mustNot().getFirst();
    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("this is a test");
  }

  @Test
  public void shouldParseMustNotTermsQueryIntoQueryBuilder() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "bool": {
                "must_not": {
                  "terms": {
                    "test_field": [
                      "this",
                      "is",
                      "a",
                      "test"
                    ]
                  }
                }
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);

    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
    assertThat(boolQueryBuilder.mustNot().size()).isEqualTo(1);
    assertThat(boolQueryBuilder.mustNot().getFirst()).isInstanceOf(TermsQueryBuilder.class);

    TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) boolQueryBuilder.mustNot().getFirst();
    assertThat(termsQueryBuilder.fieldName()).isEqualTo("test_field");
  }

  @Test
  public void shouldParseBasicFilterQueryIntoQueryBuilder() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "bool": {
                "filter": [{
                  "query_string": {
                    "query": "this is a test"
                  }
                }]
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);

    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
    assertThat(boolQueryBuilder.filter().size()).isEqualTo(1);
    assertThat(boolQueryBuilder.filter().getFirst()).isInstanceOf(QueryStringQueryBuilder.class);

    QueryStringQueryBuilder queryStringQueryBuilder =
        (QueryStringQueryBuilder) boolQueryBuilder.filter().getFirst();
    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("this is a test");
  }

  @Test
  public void shouldParseFilterTermsQueryIntoQueryBuilder() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "bool": {
                "filter": {
                  "terms": {
                    "test_field": [
                      "this",
                      "is",
                      "a",
                      "test"
                    ]
                  }
                }
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);

    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
    assertThat(boolQueryBuilder.filter().size()).isEqualTo(1);
    assertThat(boolQueryBuilder.filter().getFirst()).isInstanceOf(TermsQueryBuilder.class);

    TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) boolQueryBuilder.filter().getFirst();
    assertThat(termsQueryBuilder.fieldName()).isEqualTo("test_field");
  }

  @Test
  public void shouldParseBasicMustQueryIntoQueryBuilder() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "bool": {
                "must": {
                  "query_string": {
                    "query": "this is a test"
                  }
                }
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);

    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
    assertThat(boolQueryBuilder.must().size()).isEqualTo(1);
    assertThat(boolQueryBuilder.must().getFirst()).isInstanceOf(QueryStringQueryBuilder.class);

    QueryStringQueryBuilder queryStringQueryBuilder =
        (QueryStringQueryBuilder) boolQueryBuilder.must().getFirst();
    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("this is a test");
  }

  @Test
  public void shouldParseMustTermsQueryIntoQueryBuilder() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "bool": {
                "must": {
                  "terms": {
                    "test_field": [
                      "this",
                      "is",
                      "a",
                      "test"
                    ]
                  }
                }
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);

    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
    assertThat(boolQueryBuilder.must().size()).isEqualTo(1);
    assertThat(boolQueryBuilder.must().getFirst()).isInstanceOf(TermsQueryBuilder.class);

    TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) boolQueryBuilder.must().getFirst();
    assertThat(termsQueryBuilder.fieldName()).isEqualTo("test_field");
  }

  @Test
  public void shouldParseBasicShouldQueryIntoQueryBuilder() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "bool": {
                "should": {
                  "query_string": {
                    "query": "this is a test"
                  }
                }
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);

    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
    assertThat(boolQueryBuilder.should().size()).isEqualTo(1);
    assertThat(boolQueryBuilder.should().getFirst()).isInstanceOf(QueryStringQueryBuilder.class);

    QueryStringQueryBuilder queryStringQueryBuilder =
        (QueryStringQueryBuilder) boolQueryBuilder.should().getFirst();
    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("this is a test");
  }

  @Test
  public void shouldParseShouldTermsQueryIntoQueryBuilder() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "bool": {
                "should": {
                  "terms": {
                    "test_field": [
                      "this",
                      "is",
                      "a",
                      "test"
                    ]
                  }
                }
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);

    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
    assertThat(boolQueryBuilder.should().size()).isEqualTo(1);
    assertThat(boolQueryBuilder.should().getFirst()).isInstanceOf(TermsQueryBuilder.class);

    TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) boolQueryBuilder.should().getFirst();
    assertThat(termsQueryBuilder.fieldName()).isEqualTo("test_field");
  }

  @Test
  public void shouldParseNonBoolQueryIntoQueryBuilder() {
    AstraSearch.SearchRequest searchRequest =
        AstraSearch.SearchRequest.newBuilder()
            .setQuery(
                """
            {
              "intervals" : {
                 "my_text" : {
                   "all_of" : {
                     "ordered" : true,
                     "intervals" : [
                       {
                         "match" : {
                           "query" : "my favorite food",
                           "max_gaps" : 0,
                           "ordered" : true
                         }
                       },
                       {
                         "any_of" : {
                           "intervals" : [
                             { "match" : { "query" : "hot water" } },
                             { "match" : { "query" : "cold porridge" } }
                           ]
                         }
                       }
                   ]
                 }
               }
              }
            }""")
            .build();
    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
    assertThat(output.queryBuilder).isInstanceOf(IntervalQueryBuilder.class);

    IntervalQueryBuilder intervalQueryBuilder = (IntervalQueryBuilder) output.queryBuilder;
    assertThat(intervalQueryBuilder.getField()).isEqualTo("my_text");
  }

  @Test
  public void shouldConvertValueToFromProto() {
    assertThat(fromValueProto(toValueProto(null))).isEqualTo(null);
    assertThat(fromValueProto(toValueProto(1))).isEqualTo(1);
    assertThat(fromValueProto(toValueProto(2L))).isEqualTo(2L);
    assertThat(fromValueProto(toValueProto(3D))).isEqualTo(3D);
    assertThat(fromValueProto(toValueProto("4"))).isEqualTo("4");
    assertThat(fromValueProto(toValueProto(false))).isEqualTo(false);
    assertThat(fromValueProto(toValueProto(Map.of("1", 2, "3", false))))
        .isEqualTo(Map.of("1", 2, "3", false));
    assertThat(fromValueProto(toValueProto(List.of("1", 2, 3D, false))))
        .isEqualTo(List.of("1", 2, 3D, false));
  }

  @Test
  public void shouldConvertSchemaDefinitionToFromProto() {
    assertThat(
            SearchResultUtils.fromSchemaDefinitionProto(
                AstraSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.BOOLEAN)
                    .build()))
        .isEqualTo(FieldType.BOOLEAN);
    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.BOOLEAN))
        .isEqualTo(
            AstraSearch.SchemaDefinition.newBuilder()
                .setType(Schema.SchemaFieldType.BOOLEAN)
                .build());

    assertThat(
            SearchResultUtils.fromSchemaDefinitionProto(
                AstraSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.DOUBLE)
                    .build()))
        .isEqualTo(FieldType.DOUBLE);
    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.DOUBLE))
        .isEqualTo(
            AstraSearch.SchemaDefinition.newBuilder()
                .setType(Schema.SchemaFieldType.DOUBLE)
                .build());

    assertThat(
            SearchResultUtils.fromSchemaDefinitionProto(
                AstraSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.STRING)
                    .build()))
        .isEqualTo(FieldType.KEYWORD);
    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.STRING))
        .isEqualTo(
            AstraSearch.SchemaDefinition.newBuilder()
                .setType(Schema.SchemaFieldType.KEYWORD)
                .build());

    assertThat(
            SearchResultUtils.fromSchemaDefinitionProto(
                AstraSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.FLOAT)
                    .build()))
        .isEqualTo(FieldType.FLOAT);
    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.FLOAT))
        .isEqualTo(
            AstraSearch.SchemaDefinition.newBuilder()
                .setType(Schema.SchemaFieldType.FLOAT)
                .build());

    assertThat(
            SearchResultUtils.fromSchemaDefinitionProto(
                AstraSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.LONG)
                    .build()))
        .isEqualTo(FieldType.LONG);
    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.LONG))
        .isEqualTo(
            AstraSearch.SchemaDefinition.newBuilder().setType(Schema.SchemaFieldType.LONG).build());

    assertThat(
            SearchResultUtils.fromSchemaDefinitionProto(
                AstraSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.TEXT)
                    .build()))
        .isEqualTo(FieldType.TEXT);
    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.TEXT))
        .isEqualTo(
            AstraSearch.SchemaDefinition.newBuilder().setType(Schema.SchemaFieldType.TEXT).build());

    assertThat(
            SearchResultUtils.fromSchemaDefinitionProto(
                AstraSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.INTEGER)
                    .build()))
        .isEqualTo(FieldType.INTEGER);
    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.INTEGER))
        .isEqualTo(
            AstraSearch.SchemaDefinition.newBuilder()
                .setType(Schema.SchemaFieldType.INTEGER)
                .build());

    assertThat(
            SearchResultUtils.fromSchemaDefinitionProto(
                AstraSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.ID)
                    .build()))
        .isEqualTo(FieldType.ID);
    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.ID))
        .isEqualTo(
            AstraSearch.SchemaDefinition.newBuilder().setType(Schema.SchemaFieldType.ID).build());
  }

  @Test
  public void shouldConvertSchemaResultsToFromProto() {
    AstraSearch.SchemaResult schemaResult =
        AstraSearch.SchemaResult.newBuilder()
            .putFieldDefinition(
                "foo",
                AstraSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.TEXT)
                    .build())
            .build();

    Map<String, FieldType> fromMap = SearchResultUtils.fromSchemaResultProto(schemaResult);
    assertThat(fromMap.size()).isEqualTo(1);
    assertThat(fromMap.get("foo")).isEqualTo(FieldType.TEXT);

    AstraSearch.SchemaResult schemaResultOut = SearchResultUtils.toSchemaResultProto(fromMap);
    assertThat(schemaResult).isEqualTo(schemaResultOut);
  }
}
