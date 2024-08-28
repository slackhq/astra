// TODO FOR KYLE: FIX
//package com.slack.astra.logstore.search;
//
//import static com.slack.astra.logstore.search.SearchResultUtils.fromValueProto;
//import static com.slack.astra.logstore.search.SearchResultUtils.toValueProto;
//import static org.assertj.core.api.Assertions.assertThat;
//
//import com.slack.astra.logstore.LogMessage;
//import com.slack.astra.logstore.search.aggregations.AutoDateHistogramAggBuilder;
//import com.slack.astra.logstore.search.aggregations.AvgAggBuilder;
//import com.slack.astra.logstore.search.aggregations.CumulativeSumAggBuilder;
//import com.slack.astra.logstore.search.aggregations.DateHistogramAggBuilder;
//import com.slack.astra.logstore.search.aggregations.DerivativeAggBuilder;
//import com.slack.astra.logstore.search.aggregations.ExtendedStatsAggBuilder;
//import com.slack.astra.logstore.search.aggregations.FiltersAggBuilder;
//import com.slack.astra.logstore.search.aggregations.HistogramAggBuilder;
//import com.slack.astra.logstore.search.aggregations.MaxAggBuilder;
//import com.slack.astra.logstore.search.aggregations.MinAggBuilder;
//import com.slack.astra.logstore.search.aggregations.MovingAvgAggBuilder;
//import com.slack.astra.logstore.search.aggregations.MovingFunctionAggBuilder;
//import com.slack.astra.logstore.search.aggregations.PercentilesAggBuilder;
//import com.slack.astra.logstore.search.aggregations.SumAggBuilder;
//import com.slack.astra.logstore.search.aggregations.TermsAggBuilder;
//import com.slack.astra.logstore.search.aggregations.UniqueCountAggBuilder;
//import com.slack.astra.metadata.schema.FieldType;
//import com.slack.astra.proto.schema.Schema;
//import com.slack.astra.proto.service.AstraSearch;
//import java.time.Instant;
//import java.time.temporal.ChronoUnit;
//import java.util.List;
//import java.util.Map;
//import org.junit.jupiter.api.Test;
//import org.opensearch.index.query.BoolQueryBuilder;
//import org.opensearch.index.query.IntervalQueryBuilder;
//import org.opensearch.index.query.QueryStringQueryBuilder;
//import org.opensearch.index.query.TermsQueryBuilder;
//
//public class SearchResultUtilsTest {
//
//  @Test
//  public void shouldParseBasicMustNotQueryIntoQueryBuilder() {
//    AstraSearch.SearchRequest searchRequest =
//        AstraSearch.SearchRequest.newBuilder()
//            .setQueryString("")
//            .setQuery(
//                """
//            {
//              "bool": {
//                "must_not": {
//                  "query_string": {
//                    "query": "this is a test"
//                  }
//                }
//              }
//            }""")
//            .build();
//    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
//    assertThat(output.queryStr).isEmpty();
//    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);
//
//    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
//    assertThat(boolQueryBuilder.mustNot().size()).isEqualTo(1);
//    assertThat(boolQueryBuilder.mustNot().getFirst()).isInstanceOf(QueryStringQueryBuilder.class);
//
//    QueryStringQueryBuilder queryStringQueryBuilder =
//        (QueryStringQueryBuilder) boolQueryBuilder.mustNot().getFirst();
//    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("this is a test");
//  }
//
//  @Test
//  public void shouldParseMustNotTermsQueryIntoQueryBuilder() {
//    AstraSearch.SearchRequest searchRequest =
//        AstraSearch.SearchRequest.newBuilder()
//            .setQueryString("")
//            .setQuery(
//                """
//            {
//              "bool": {
//                "must_not": {
//                  "terms": {
//                    "test_field": [
//                      "this",
//                      "is",
//                      "a",
//                      "test"
//                    ]
//                  }
//                }
//              }
//            }""")
//            .build();
//    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
//    assertThat(output.queryStr).isEmpty();
//    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);
//
//    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
//    assertThat(boolQueryBuilder.mustNot().size()).isEqualTo(1);
//    assertThat(boolQueryBuilder.mustNot().getFirst()).isInstanceOf(TermsQueryBuilder.class);
//
//    TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) boolQueryBuilder.mustNot().getFirst();
//    assertThat(termsQueryBuilder.fieldName()).isEqualTo("test_field");
//  }
//
//  @Test
//  public void shouldParseBasicFilterQueryIntoQueryBuilder() {
//    AstraSearch.SearchRequest searchRequest =
//        AstraSearch.SearchRequest.newBuilder()
//            .setQueryString("")
//            .setQuery(
//                """
//            {
//              "bool": {
//                "filter": [{
//                  "query_string": {
//                    "query": "this is a test"
//                  }
//                }]
//              }
//            }""")
//            .build();
//    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
//    assertThat(output.queryStr).isEmpty();
//    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);
//
//    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
//    assertThat(boolQueryBuilder.filter().size()).isEqualTo(1);
//    assertThat(boolQueryBuilder.filter().getFirst()).isInstanceOf(QueryStringQueryBuilder.class);
//
//    QueryStringQueryBuilder queryStringQueryBuilder =
//        (QueryStringQueryBuilder) boolQueryBuilder.filter().getFirst();
//    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("this is a test");
//  }
//
//  @Test
//  public void shouldParseFilterTermsQueryIntoQueryBuilder() {
//    AstraSearch.SearchRequest searchRequest =
//        AstraSearch.SearchRequest.newBuilder()
//            .setQueryString("")
//            .setQuery(
//                """
//            {
//              "bool": {
//                "filter": {
//                  "terms": {
//                    "test_field": [
//                      "this",
//                      "is",
//                      "a",
//                      "test"
//                    ]
//                  }
//                }
//              }
//            }""")
//            .build();
//    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
//    assertThat(output.queryStr).isEmpty();
//    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);
//
//    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
//    assertThat(boolQueryBuilder.filter().size()).isEqualTo(1);
//    assertThat(boolQueryBuilder.filter().getFirst()).isInstanceOf(TermsQueryBuilder.class);
//
//    TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) boolQueryBuilder.filter().getFirst();
//    assertThat(termsQueryBuilder.fieldName()).isEqualTo("test_field");
//  }
//
//  @Test
//  public void shouldParseBasicMustQueryIntoQueryBuilder() {
//    AstraSearch.SearchRequest searchRequest =
//        AstraSearch.SearchRequest.newBuilder()
//            .setQueryString("")
//            .setQuery(
//                """
//            {
//              "bool": {
//                "must": {
//                  "query_string": {
//                    "query": "this is a test"
//                  }
//                }
//              }
//            }""")
//            .build();
//    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
//    assertThat(output.queryStr).isEmpty();
//    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);
//
//    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
//    assertThat(boolQueryBuilder.must().size()).isEqualTo(1);
//    assertThat(boolQueryBuilder.must().getFirst()).isInstanceOf(QueryStringQueryBuilder.class);
//
//    QueryStringQueryBuilder queryStringQueryBuilder =
//        (QueryStringQueryBuilder) boolQueryBuilder.must().getFirst();
//    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("this is a test");
//  }
//
//  @Test
//  public void shouldParseMustTermsQueryIntoQueryBuilder() {
//    AstraSearch.SearchRequest searchRequest =
//        AstraSearch.SearchRequest.newBuilder()
//            .setQueryString("")
//            .setQuery(
//                """
//            {
//              "bool": {
//                "must": {
//                  "terms": {
//                    "test_field": [
//                      "this",
//                      "is",
//                      "a",
//                      "test"
//                    ]
//                  }
//                }
//              }
//            }""")
//            .build();
//    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
//    assertThat(output.queryStr).isEmpty();
//    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);
//
//    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
//    assertThat(boolQueryBuilder.must().size()).isEqualTo(1);
//    assertThat(boolQueryBuilder.must().getFirst()).isInstanceOf(TermsQueryBuilder.class);
//
//    TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) boolQueryBuilder.must().getFirst();
//    assertThat(termsQueryBuilder.fieldName()).isEqualTo("test_field");
//  }
//
//  @Test
//  public void shouldParseBasicShouldQueryIntoQueryBuilder() {
//    AstraSearch.SearchRequest searchRequest =
//        AstraSearch.SearchRequest.newBuilder()
//            .setQueryString("")
//            .setQuery(
//                """
//            {
//              "bool": {
//                "should": {
//                  "query_string": {
//                    "query": "this is a test"
//                  }
//                }
//              }
//            }""")
//            .build();
//    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
//    assertThat(output.queryStr).isEmpty();
//    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);
//
//    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
//    assertThat(boolQueryBuilder.should().size()).isEqualTo(1);
//    assertThat(boolQueryBuilder.should().getFirst()).isInstanceOf(QueryStringQueryBuilder.class);
//
//    QueryStringQueryBuilder queryStringQueryBuilder =
//        (QueryStringQueryBuilder) boolQueryBuilder.should().getFirst();
//    assertThat(queryStringQueryBuilder.queryString()).isEqualTo("this is a test");
//  }
//
//  @Test
//  public void shouldParseShouldTermsQueryIntoQueryBuilder() {
//    AstraSearch.SearchRequest searchRequest =
//        AstraSearch.SearchRequest.newBuilder()
//            .setQueryString("")
//            .setQuery(
//                """
//            {
//              "bool": {
//                "should": {
//                  "terms": {
//                    "test_field": [
//                      "this",
//                      "is",
//                      "a",
//                      "test"
//                    ]
//                  }
//                }
//              }
//            }""")
//            .build();
//    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
//    assertThat(output.queryStr).isEmpty();
//    assertThat(output.queryBuilder).isInstanceOf(BoolQueryBuilder.class);
//
//    BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) output.queryBuilder;
//    assertThat(boolQueryBuilder.should().size()).isEqualTo(1);
//    assertThat(boolQueryBuilder.should().getFirst()).isInstanceOf(TermsQueryBuilder.class);
//
//    TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) boolQueryBuilder.should().getFirst();
//    assertThat(termsQueryBuilder.fieldName()).isEqualTo("test_field");
//  }
//
//  @Test
//  public void shouldParseNonBoolQueryIntoQueryBuilder() {
//    AstraSearch.SearchRequest searchRequest =
//        AstraSearch.SearchRequest.newBuilder()
//            .setQueryString("")
//            .setQuery(
//                """
//            {
//              "intervals" : {
//                 "my_text" : {
//                   "all_of" : {
//                     "ordered" : true,
//                     "intervals" : [
//                       {
//                         "match" : {
//                           "query" : "my favorite food",
//                           "max_gaps" : 0,
//                           "ordered" : true
//                         }
//                       },
//                       {
//                         "any_of" : {
//                           "intervals" : [
//                             { "match" : { "query" : "hot water" } },
//                             { "match" : { "query" : "cold porridge" } }
//                           ]
//                         }
//                       }
//                   ]
//                 }
//               }
//              }
//            }""")
//            .build();
//    SearchQuery output = SearchResultUtils.fromSearchRequest(searchRequest);
//    assertThat(output.queryStr).isEmpty();
//    assertThat(output.queryBuilder).isInstanceOf(IntervalQueryBuilder.class);
//
//    IntervalQueryBuilder intervalQueryBuilder = (IntervalQueryBuilder) output.queryBuilder;
//    assertThat(intervalQueryBuilder.getField()).isEqualTo("my_text");
//  }
//
//  @Test
//  public void shouldConvertMinAggToFromProto() {
//    MinAggBuilder minAggBuilder =
//        new MinAggBuilder("1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "3", "return 8;");
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(minAggBuilder);
//    MinAggBuilder otherMinAggBuilder =
//        (MinAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(minAggBuilder).isEqualTo(otherMinAggBuilder);
//  }
//
//  @Test
//  public void shouldConvertMaxAggToFromProto() {
//    MaxAggBuilder maxAggBuilder =
//        new MaxAggBuilder("1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "3", "return 7;");
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(maxAggBuilder);
//    MaxAggBuilder otherMaxAggBuilder =
//        (MaxAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(maxAggBuilder).isEqualTo(otherMaxAggBuilder);
//  }
//
//  @Test
//  public void shouldConvertAvgAggToFromProto() {
//    AvgAggBuilder avgAggBuilder1 =
//        new AvgAggBuilder("1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "3", "return 9;");
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(avgAggBuilder1);
//    AvgAggBuilder avgAggBuilder2 =
//        (AvgAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(avgAggBuilder1).isEqualTo(avgAggBuilder2);
//    assertThat(avgAggBuilder1).isEqualTo(avgAggBuilder2);
//    assertThat(avgAggBuilder1.getName()).isEqualTo("1");
//    assertThat(avgAggBuilder1.getField())
//        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
//    assertThat(avgAggBuilder1.getMissing()).isEqualTo("3");
//    assertThat(avgAggBuilder1.getScript()).isEqualTo("return 9;");
//  }
//
//  @Test
//  public void shouldConvertSumAggToFromProto() {
//    SumAggBuilder sumAggBuilder1 =
//        new SumAggBuilder("1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "3", "return 9;");
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(sumAggBuilder1);
//    SumAggBuilder sumAggBuilder2 =
//        (SumAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(sumAggBuilder1).isEqualTo(sumAggBuilder2);
//    assertThat(sumAggBuilder1.getName()).isEqualTo("1");
//    assertThat(sumAggBuilder1.getField())
//        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
//    assertThat(sumAggBuilder1.getMissing()).isEqualTo("3");
//    assertThat(sumAggBuilder1.getScript()).isEqualTo("return 9;");
//  }
//
//  @Test
//  public void shouldConvertExtendedStatsAggToFromProto() {
//    ExtendedStatsAggBuilder extendedStatsAggBuilder1 =
//        new ExtendedStatsAggBuilder(
//            "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "3", "return 9;", 3D);
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(extendedStatsAggBuilder1);
//    ExtendedStatsAggBuilder extendedStatsAggBuilder2 =
//        (ExtendedStatsAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(extendedStatsAggBuilder1).isEqualTo(extendedStatsAggBuilder2);
//    assertThat(extendedStatsAggBuilder1.getName()).isEqualTo("1");
//    assertThat(extendedStatsAggBuilder1.getField())
//        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
//    assertThat(extendedStatsAggBuilder1.getMissing()).isEqualTo("3");
//    assertThat(extendedStatsAggBuilder1.getScript()).isEqualTo("return 9;");
//    assertThat(extendedStatsAggBuilder1.getSigma()).isEqualTo(3D);
//  }
//
//  @Test
//  public void shouldConvertDateHistogramAggToFromProto() {
//    DateHistogramAggBuilder dateHistogramAggBuilder1 =
//        new DateHistogramAggBuilder(
//            "1",
//            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
//            "5s",
//            "2s",
//            null,
//            10000,
//            "epoch_ms",
//            Map.of(
//                "min", Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
//                "max", Instant.now().toEpochMilli()),
//            List.of());
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(dateHistogramAggBuilder1);
//    DateHistogramAggBuilder dateHistogramAggBuilder2 =
//        (DateHistogramAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(dateHistogramAggBuilder1).isEqualTo(dateHistogramAggBuilder2);
//  }
//
//  @Test
//  public void shouldConvertAutoDateHistogramAggToFromProto() {
//    AutoDateHistogramAggBuilder autoDateHistogramAggBuilder1 =
//        new AutoDateHistogramAggBuilder(
//            "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "day", 10, List.of());
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(autoDateHistogramAggBuilder1);
//    AutoDateHistogramAggBuilder autoDateHistogramAggBuilder2 =
//        (AutoDateHistogramAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(autoDateHistogramAggBuilder1).isEqualTo(autoDateHistogramAggBuilder2);
//
//    AutoDateHistogramAggBuilder autoDateHistogramAggBuilder3 =
//        new AutoDateHistogramAggBuilder(
//            "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, null, null, List.of());
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation2 =
//        SearchResultUtils.toSearchAggregationProto(autoDateHistogramAggBuilder3);
//    AutoDateHistogramAggBuilder autoDateHistogramAggBuilder4 =
//        (AutoDateHistogramAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation2);
//
//    assertThat(autoDateHistogramAggBuilder3).isEqualTo(autoDateHistogramAggBuilder4);
//  }
//
//  @Test
//  public void shouldConvertHistogramAggToFromProto() {
//    HistogramAggBuilder histogramAggBuilder1 =
//        new HistogramAggBuilder("1", "@timestamp", "1000", 1, List.of());
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(histogramAggBuilder1);
//    HistogramAggBuilder histogramAggBuilder2 =
//        (HistogramAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(histogramAggBuilder1).isEqualTo(histogramAggBuilder2);
//  }
//
//  @Test
//  public void shouldConvertTermsAggToFromProto() {
//    TermsAggBuilder termsAggBuilder1 =
//        new TermsAggBuilder(
//            "foo",
//            List.of(),
//            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
//            3,
//            100,
//            0,
//            Map.of("_term", "asc"));
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(termsAggBuilder1);
//    TermsAggBuilder termsAggBuilder2 =
//        (TermsAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(termsAggBuilder1).isEqualTo(termsAggBuilder2);
//  }
//
//  @Test
//  public void shouldConvertUniqueCountToFromProto() {
//    UniqueCountAggBuilder uniqueCountAggBuilder1 =
//        new UniqueCountAggBuilder("foo", "service_name", "2", 1L);
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(uniqueCountAggBuilder1);
//    UniqueCountAggBuilder uniqueCountAggBuilder2 =
//        (UniqueCountAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(uniqueCountAggBuilder1).isEqualTo(uniqueCountAggBuilder2);
//  }
//
//  @Test
//  public void shouldConvertPercentilesToFromProto() {
//    PercentilesAggBuilder percentilesAggBuilder1 =
//        new PercentilesAggBuilder("foo", "service_name", "2", List.of(99D, 100D), "return 8;");
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(percentilesAggBuilder1);
//    PercentilesAggBuilder percentilesAggBuilder2 =
//        (PercentilesAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(percentilesAggBuilder1).isEqualTo(percentilesAggBuilder2);
//  }
//
//  @Test
//  public void shouldConvertCumulativeSumPipelineToFromProto() {
//    CumulativeSumAggBuilder cumulativeSumAggBuilder1 =
//        new CumulativeSumAggBuilder("2", "_count", "##0.#####E0");
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(cumulativeSumAggBuilder1);
//    CumulativeSumAggBuilder cumulativeSumAggBuilder2 =
//        (CumulativeSumAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(cumulativeSumAggBuilder1).isEqualTo(cumulativeSumAggBuilder2);
//    assertThat(cumulativeSumAggBuilder1.getName()).isEqualTo("2");
//    assertThat(cumulativeSumAggBuilder1.getBucketsPath()).isEqualTo("_count");
//    assertThat(cumulativeSumAggBuilder1.getFormat()).isEqualTo("##0.#####E0");
//  }
//
//  @Test
//  public void shouldConvertMovingFunctionPipelineToFromProto() {
//    MovingFunctionAggBuilder movingFunctionAggBuilder1 =
//        new MovingFunctionAggBuilder("2", "_count", "return 8;", 2, 3);
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(movingFunctionAggBuilder1);
//    MovingFunctionAggBuilder movingFunctionAggBuilder2 =
//        (MovingFunctionAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(movingFunctionAggBuilder1).isEqualTo(movingFunctionAggBuilder2);
//    assertThat(movingFunctionAggBuilder1.getName()).isEqualTo("2");
//    assertThat(movingFunctionAggBuilder1.getBucketsPath()).isEqualTo("_count");
//    assertThat(movingFunctionAggBuilder1.getScript()).isEqualTo("return 8;");
//    assertThat(movingFunctionAggBuilder1.getWindow()).isEqualTo(2);
//    assertThat(movingFunctionAggBuilder1.getShift()).isEqualTo(3);
//  }
//
//  @Test
//  public void shouldConvertMovingAveragePipelineToFromProto() {
//    MovingAvgAggBuilder movingAvgAggBuilder1 =
//        new MovingAvgAggBuilder("1", "_count", "linear", 2, 5, 3D, 4D, 5D, 2, false, true);
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(movingAvgAggBuilder1);
//    MovingAvgAggBuilder movingAvgAggBuilder2 =
//        (MovingAvgAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(movingAvgAggBuilder1).isEqualTo(movingAvgAggBuilder2);
//    assertThat(movingAvgAggBuilder1.getName()).isEqualTo("1");
//    assertThat(movingAvgAggBuilder1.getBucketsPath()).isEqualTo("_count");
//    assertThat(movingAvgAggBuilder1.getModel()).isEqualTo("linear");
//    assertThat(movingAvgAggBuilder1.getWindow()).isEqualTo(2);
//    assertThat(movingAvgAggBuilder1.getPredict()).isEqualTo(5);
//    assertThat(movingAvgAggBuilder1.getAlpha()).isEqualTo(3);
//    assertThat(movingAvgAggBuilder1.getBeta()).isEqualTo(4);
//    assertThat(movingAvgAggBuilder1.getGamma()).isEqualTo(5);
//    assertThat(movingAvgAggBuilder1.getPeriod()).isEqualTo(2);
//    assertThat(movingAvgAggBuilder1.isPad()).isEqualTo(false);
//    assertThat(movingAvgAggBuilder1.isMinimize()).isEqualTo(true);
//  }
//
//  @Test
//  public void shouldConvertDerivativePipelineToFromProto() {
//    DerivativeAggBuilder derivativeAggBuilder1 = new DerivativeAggBuilder("2", "_count", "unit");
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(derivativeAggBuilder1);
//    DerivativeAggBuilder derivativeAggBuilder2 =
//        (DerivativeAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(derivativeAggBuilder1).isEqualTo(derivativeAggBuilder2);
//    assertThat(derivativeAggBuilder1.getName()).isEqualTo("2");
//    assertThat(derivativeAggBuilder1.getBucketsPath()).isEqualTo("_count");
//    assertThat(derivativeAggBuilder1.getUnit()).isEqualTo("unit");
//  }
//
//  @Test
//  public void shouldConvertNestedAggregations() {
//    // this is not representative of a real or reasonable query, but we should be able to convert it
//    // just the same
//    AvgAggBuilder avgAggBuilder =
//        new AvgAggBuilder("1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, null, null);
//
//    DateHistogramAggBuilder dateHistogramAggBuilderInner =
//        new DateHistogramAggBuilder(
//            "1",
//            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
//            "5s",
//            "2s",
//            null,
//            10000,
//            "epoch_ms",
//            Map.of(
//                "min", Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
//                "max", Instant.now().toEpochMilli()),
//            List.of(avgAggBuilder));
//
//    DateHistogramAggBuilder dateHistogramAggBuilder1 =
//        new DateHistogramAggBuilder(
//            "2",
//            "duration_ms",
//            "10s",
//            "7s",
//            null,
//            1000,
//            "epoch_ms",
//            Map.of(
//                "min", Instant.now().minus(2, ChronoUnit.MINUTES).toEpochMilli(),
//                "max", Instant.now().plus(2, ChronoUnit.MINUTES).toEpochMilli()),
//            List.of(dateHistogramAggBuilderInner));
//
//    AstraSearch.SearchRequest.SearchAggregation searchAggregation =
//        SearchResultUtils.toSearchAggregationProto(dateHistogramAggBuilder1);
//    DateHistogramAggBuilder dateHistogramAggBuilder2 =
//        (DateHistogramAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);
//
//    assertThat(dateHistogramAggBuilder1).isEqualTo(dateHistogramAggBuilder2);
//    assertThat(dateHistogramAggBuilder1.getSubAggregations())
//        .isEqualTo(dateHistogramAggBuilder2.getSubAggregations());
//  }
//
//  @Test
//  public void shouldConvertValueToFromProto() {
//    assertThat(fromValueProto(toValueProto(null))).isEqualTo(null);
//    assertThat(fromValueProto(toValueProto(1))).isEqualTo(1);
//    assertThat(fromValueProto(toValueProto(2L))).isEqualTo(2L);
//    assertThat(fromValueProto(toValueProto(3D))).isEqualTo(3D);
//    assertThat(fromValueProto(toValueProto("4"))).isEqualTo("4");
//    assertThat(fromValueProto(toValueProto(false))).isEqualTo(false);
//    assertThat(fromValueProto(toValueProto(Map.of("1", 2, "3", false))))
//        .isEqualTo(Map.of("1", 2, "3", false));
//    assertThat(fromValueProto(toValueProto(List.of("1", 2, 3D, false))))
//        .isEqualTo(List.of("1", 2, 3D, false));
//  }
//
//  @Test
//  public void shouldConvertSchemaDefinitionToFromProto() {
//    assertThat(
//            SearchResultUtils.fromSchemaDefinitionProto(
//                AstraSearch.SchemaDefinition.newBuilder()
//                    .setType(Schema.SchemaFieldType.BOOLEAN)
//                    .build()))
//        .isEqualTo(FieldType.BOOLEAN);
//    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.BOOLEAN))
//        .isEqualTo(
//            AstraSearch.SchemaDefinition.newBuilder()
//                .setType(Schema.SchemaFieldType.BOOLEAN)
//                .build());
//
//    assertThat(
//            SearchResultUtils.fromSchemaDefinitionProto(
//                AstraSearch.SchemaDefinition.newBuilder()
//                    .setType(Schema.SchemaFieldType.DOUBLE)
//                    .build()))
//        .isEqualTo(FieldType.DOUBLE);
//    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.DOUBLE))
//        .isEqualTo(
//            AstraSearch.SchemaDefinition.newBuilder()
//                .setType(Schema.SchemaFieldType.DOUBLE)
//                .build());
//
//    assertThat(
//            SearchResultUtils.fromSchemaDefinitionProto(
//                AstraSearch.SchemaDefinition.newBuilder()
//                    .setType(Schema.SchemaFieldType.STRING)
//                    .build()))
//        .isEqualTo(FieldType.KEYWORD);
//    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.STRING))
//        .isEqualTo(
//            AstraSearch.SchemaDefinition.newBuilder()
//                .setType(Schema.SchemaFieldType.KEYWORD)
//                .build());
//
//    assertThat(
//            SearchResultUtils.fromSchemaDefinitionProto(
//                AstraSearch.SchemaDefinition.newBuilder()
//                    .setType(Schema.SchemaFieldType.FLOAT)
//                    .build()))
//        .isEqualTo(FieldType.FLOAT);
//    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.FLOAT))
//        .isEqualTo(
//            AstraSearch.SchemaDefinition.newBuilder()
//                .setType(Schema.SchemaFieldType.FLOAT)
//                .build());
//
//    assertThat(
//            SearchResultUtils.fromSchemaDefinitionProto(
//                AstraSearch.SchemaDefinition.newBuilder()
//                    .setType(Schema.SchemaFieldType.LONG)
//                    .build()))
//        .isEqualTo(FieldType.LONG);
//    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.LONG))
//        .isEqualTo(
//            AstraSearch.SchemaDefinition.newBuilder().setType(Schema.SchemaFieldType.LONG).build());
//
//    assertThat(
//            SearchResultUtils.fromSchemaDefinitionProto(
//                AstraSearch.SchemaDefinition.newBuilder()
//                    .setType(Schema.SchemaFieldType.TEXT)
//                    .build()))
//        .isEqualTo(FieldType.TEXT);
//    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.TEXT))
//        .isEqualTo(
//            AstraSearch.SchemaDefinition.newBuilder().setType(Schema.SchemaFieldType.TEXT).build());
//
//    assertThat(
//            SearchResultUtils.fromSchemaDefinitionProto(
//                AstraSearch.SchemaDefinition.newBuilder()
//                    .setType(Schema.SchemaFieldType.INTEGER)
//                    .build()))
//        .isEqualTo(FieldType.INTEGER);
//    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.INTEGER))
//        .isEqualTo(
//            AstraSearch.SchemaDefinition.newBuilder()
//                .setType(Schema.SchemaFieldType.INTEGER)
//                .build());
//
//    assertThat(
//            SearchResultUtils.fromSchemaDefinitionProto(
//                AstraSearch.SchemaDefinition.newBuilder()
//                    .setType(Schema.SchemaFieldType.ID)
//                    .build()))
//        .isEqualTo(FieldType.ID);
//    assertThat(SearchResultUtils.toSchemaDefinitionProto(FieldType.ID))
//        .isEqualTo(
//            AstraSearch.SchemaDefinition.newBuilder().setType(Schema.SchemaFieldType.ID).build());
//  }
//
//  @Test
//  public void shouldConvertSchemaResultsToFromProto() {
//    AstraSearch.SchemaResult schemaResult =
//        AstraSearch.SchemaResult.newBuilder()
//            .putFieldDefinition(
//                "foo",
//                AstraSearch.SchemaDefinition.newBuilder()
//                    .setType(Schema.SchemaFieldType.TEXT)
//                    .build())
//            .build();
//
//    Map<String, FieldType> fromMap = SearchResultUtils.fromSchemaResultProto(schemaResult);
//    assertThat(fromMap.size()).isEqualTo(1);
//    assertThat(fromMap.get("foo")).isEqualTo(FieldType.TEXT);
//
//    AstraSearch.SchemaResult schemaResultOut = SearchResultUtils.toSchemaResultProto(fromMap);
//    assertThat(schemaResult).isEqualTo(schemaResultOut);
//  }
//
//  @Test
//  public void shouldConvertFiltersAggregationToFromProto() {
//    AstraSearch.SearchRequest.SearchAggregation.FiltersAggregation filterAggregation =
//        AstraSearch.SearchRequest.SearchAggregation.FiltersAggregation.newBuilder()
//            .putAllFilters(
//                Map.of(
//                    "foo",
//                    AstraSearch.SearchRequest.SearchAggregation.FilterAggregation.newBuilder()
//                        .setQueryString("*:*")
//                        .setAnalyzeWildcard(true)
//                        .build()))
//            .build();
//
//    Map<String, FiltersAggBuilder.FilterAgg> filterAggMap =
//        SearchResultUtils.fromFiltersAggregation(filterAggregation);
//    AstraSearch.SearchRequest.SearchAggregation.FiltersAggregation fromMap =
//        SearchResultUtils.toFiltersAggregation(filterAggMap);
//
//    assertThat(filterAggregation).isEqualTo(fromMap);
//    assertThat(filterAggregation.getFiltersMap().size()).isEqualTo(1);
//    assertThat(filterAggregation.getFiltersMap().get("foo").getQueryString()).isEqualTo("*:*");
//    assertThat(filterAggregation.getFiltersMap().get("foo").getAnalyzeWildcard()).isEqualTo(true);
//  }
//}
