package com.slack.astra.elasticsearchApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.proto.service.AstraSearch;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class OpenSearchRequestTest {

  private static ObjectMapper objectMapper;

  @BeforeAll
  public static void beforeClass() {
    objectMapper = new ObjectMapper();
  }

  private String getRawQueryString(String filename) throws IOException {
    return Resources.toString(
        Resources.getResource(String.format("opensearchRequest/%s.ndjson", filename)),
        Charset.defaultCharset());
  }

  @Test
  public void testSourceIncludesBooleanFilter() throws Exception {
    String rawRequest = getRawQueryString("boolean_source_includes_filter");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(500);
    assertThat(request.getQueryString()).isEqualTo("*:*");
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);

    // Assert that the includes fields are set correctly
    assertThat(request.getIncludeFields().getFieldsMap()).isEmpty();
    assertThat(request.getIncludeFields().getWildcardsCount()).isZero();
    assertThat(request.getIncludeFields().hasAll()).isTrue();
    assertThat(request.getIncludeFields().getAll()).isTrue();

    // Assert that the excludes fields are set correctly
    assertThat(request.getExcludeFields().getFieldsMap()).isEmpty();
    assertThat(request.getExcludeFields().getWildcardsCount()).isZero();
    assertThat(request.getExcludeFields().hasAll()).isFalse();

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(request.getQuery()).isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testSourceIncludesListFilter() throws Exception {
    String rawRequest = getRawQueryString("list_source_includes_filter");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(500);
    assertThat(request.getQueryString()).isEqualTo("*:*");
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);

    // Assert that the includes fields are set correctly
    assertThat(request.getIncludeFields().getFieldsMap()).hasSize(1);
    assertThat(request.getIncludeFields().getFieldsMap().get("normal_field_test")).isTrue();
    assertThat(request.getIncludeFields().getWildcardsCount()).isOne();
    assertThat(request.getIncludeFields().getWildcards(0)).isEqualTo("wildcard_test.*");
    assertThat(request.getIncludeFields().hasAll()).isFalse();

    // Assert that the excludes fields are set correctly
    assertThat(request.getExcludeFields().getFieldsMap()).isEmpty();
    assertThat(request.getExcludeFields().getWildcardsCount()).isZero();
    assertThat(request.getExcludeFields().hasAll()).isFalse();

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(request.getQuery()).isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testSourceIncludesObjectFilter() throws Exception {
    String rawRequest = getRawQueryString("object_source_includes_filter");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(500);
    assertThat(request.getQueryString()).isEqualTo("*:*");
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);

    // Assert that the includes fields are set correctly
    assertThat(request.getIncludeFields().getFieldsMap()).hasSize(1);
    assertThat(request.getIncludeFields().getFieldsMap().get("include_normal_field_test")).isTrue();
    assertThat(request.getIncludeFields().getWildcardsCount()).isOne();
    assertThat(request.getIncludeFields().getWildcards(0)).isEqualTo("include_wildcard_test.*");
    assertThat(request.getIncludeFields().hasAll()).isFalse();

    // Assert that the excludes fields are set correctly
    assertThat(request.getExcludeFields().getFieldsMap()).hasSize(1);
    assertThat(request.getExcludeFields().getFieldsMap().get("exclude_normal_field_test")).isTrue();
    assertThat(request.getExcludeFields().getWildcardsCount()).isOne();
    assertThat(request.getExcludeFields().getWildcards(0)).isEqualTo("exclude_wildcard_test.*");
    assertThat(request.getExcludeFields().hasAll()).isFalse();

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(request.getQuery()).isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testNoAggs() throws Exception {
    String rawRequest = getRawQueryString("noaggs");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(500);
    assertThat(request.getQueryString()).isEqualTo("*:*");
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(request.getQuery()).isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testGeneralFields() throws Exception {
    String rawRequest = getRawQueryString("datehistogram");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(1);
    assertThat(request.getQueryString()).isEqualTo("*:*");
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1676498801027L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1676500240688L);

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(request.getQuery()).isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testDateHistogram() throws Exception {
    String rawRequest = getRawQueryString("datehistogram");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();

    assertThat(dateHistogramAggBuilder.getName()).isEqualTo("2");
    assertThat(dateHistogramAggBuilder.getValueSource().getField())
        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(0);

    AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.DateHistogramAggregation
        dateHistogramAggregation = dateHistogramAggBuilder.getValueSource().getDateHistogram();
    assertThat(dateHistogramAggregation.getInterval()).isEqualTo("10s");
    assertThat(dateHistogramAggregation.getMinDocCount()).isEqualTo(90000);
    assertThat(dateHistogramAggregation.getExtendedBoundsMap())
        .isEqualTo(
            Map.of(
                "min", 1676498801027L,
                "max", 1676500240688L));
    assertThat(dateHistogramAggregation.getFormat()).isEqualTo("epoch_millis");
    assertThat(dateHistogramAggregation.getOffset()).isEqualTo("5s");

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testHistogram() throws Exception {
    String rawRequest = getRawQueryString("histogram");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation histogramAggBuilder =
        parsedRequestList.get(0).getAggregations();

    assertThat(histogramAggBuilder.getName()).isEqualTo("2");
    assertThat(histogramAggBuilder.getValueSource().getField())
        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
    assertThat(histogramAggBuilder.getSubAggregationsCount()).isEqualTo(0);

    AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.HistogramAggregation
        histogramAggregation = histogramAggBuilder.getValueSource().getHistogram();
    assertThat(histogramAggregation.getInterval()).isEqualTo("1000");
    assertThat(histogramAggregation.getMinDocCount()).isEqualTo(1);

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testUniqueCount() throws Exception {
    String rawRequest = getRawQueryString("unique_count");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation uniqueCountAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);

    assertThat(uniqueCountAggBuilder.getType()).isEqualTo("cardinality");
    assertThat(uniqueCountAggBuilder.getName()).isEqualTo("1");
    assertThat(uniqueCountAggBuilder.getValueSource().getField()).isEqualTo("service_name");
    assertThat(uniqueCountAggBuilder.getValueSource().getMissing().hasNullValue()).isTrue();
    assertThat(
            uniqueCountAggBuilder
                .getValueSource()
                .getUniqueCount()
                .getPrecisionThreshold()
                .getLongValue())
        .isEqualTo(1);

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testPercentiles() throws Exception {
    String rawRequest = getRawQueryString("percentiles");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation percentileAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);
    assertThat(percentileAggBuilder.getType()).isEqualTo("percentiles");
    assertThat(percentileAggBuilder.getName()).isEqualTo("5");
    assertThat(percentileAggBuilder.getValueSource().getField()).isEqualTo("service_name");
    assertThat(percentileAggBuilder.getValueSource().getMissing().hasNullValue()).isTrue();
    assertThat(percentileAggBuilder.getValueSource().getPercentiles().getPercentilesList())
        .containsExactly(25D, 50D, 75D, 95D, 99D);
    assertThat(percentileAggBuilder.getValueSource().getScript().getStringValue())
        .isEqualTo("return 8;");

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testHistogramWithNestedExtendedStats() throws Exception {
    String rawRequest = getRawQueryString("nested_datehistogram_extended_stats");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getValueSource().getField())
        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);
    AstraSearch.SearchRequest.SearchAggregation extendedStatsAgg =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);

    assertThat(extendedStatsAgg.getName()).isEqualTo("1");
    assertThat(extendedStatsAgg.getValueSource().getField()).isEqualTo("duration_ms");
    assertThat(extendedStatsAgg.getValueSource().getExtendedStats().getSigma().getDoubleValue())
        .isEqualTo(2D);

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testHistogramWithNestedCumulativeSum() throws Exception {
    String rawRequest = getRawQueryString("nested_datehistogram_cumulative_sum");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getValueSource().getField())
        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation cumulativeSumAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);
    assertThat(cumulativeSumAggBuilder.getName()).isEqualTo("3");
    assertThat(cumulativeSumAggBuilder.getPipeline().getBucketsPath()).isEqualTo("_count");
    assertThat(
            cumulativeSumAggBuilder.getPipeline().getCumulativeSum().getFormat().getStringValue())
        .isEqualTo("##0.#####E0");

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testHistogramWithNestedMovingFunction() throws Exception {
    String rawRequest = getRawQueryString("nested_datehistogram_moving_fn");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getValueSource().getField())
        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation movingFunctionAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);
    assertThat(movingFunctionAggBuilder.getName()).isEqualTo("3");
    assertThat(movingFunctionAggBuilder.getPipeline().getBucketsPath()).isEqualTo("_count");
    assertThat(movingFunctionAggBuilder.getPipeline().getMovingFunction().getScript())
        .isEqualTo("return 8;");
    assertThat(movingFunctionAggBuilder.getPipeline().getMovingFunction().getWindow()).isEqualTo(2);
    assertThat(movingFunctionAggBuilder.getPipeline().getMovingFunction().getShift()).isEqualTo(3);

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testHistogramWithNestedDerivative() throws Exception {
    String rawRequest = getRawQueryString("nested_datehistogram_derivative");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getValueSource().getField())
        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation derivativeAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);
    assertThat(derivativeAggBuilder.getName()).isEqualTo("3");
    assertThat(derivativeAggBuilder.getPipeline().getBucketsPath()).isEqualTo("_count");
    assertThat(derivativeAggBuilder.getPipeline().getDerivative().getUnit().getStringValue())
        .isEqualTo("1m");

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testHistogramWithNestedAvg() throws Exception {
    String rawRequest = getRawQueryString("nested_datehistogram_avg");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getValueSource().getField())
        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.DateHistogramAggregation
        dateHistogramAggregation = dateHistogramAggBuilder.getValueSource().getDateHistogram();
    assertThat(dateHistogramAggregation.getInterval()).isEqualTo("10s");
    assertThat(dateHistogramAggregation.getMinDocCount()).isEqualTo(90000);
    assertThat(dateHistogramAggregation.getExtendedBoundsMap())
        .isEqualTo(
            Map.of(
                "min", 1676498801027L,
                "max", 1676500240688L));
    assertThat(dateHistogramAggregation.getFormat()).isEqualTo("epoch_millis");
    assertThat(dateHistogramAggregation.getOffset()).isEqualTo("5s");

    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);
    AstraSearch.SearchRequest.SearchAggregation avgAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);
    assertThat(avgAggBuilder.getName()).isEqualTo("3");
    assertThat(avgAggBuilder.getValueSource().getField()).isEqualTo("duration_ms");

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testHistogramWithNestedSum() throws Exception {
    String rawRequest = getRawQueryString("nested_datehistogram_sum");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getValueSource().getField())
        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);
    AstraSearch.SearchRequest.SearchAggregation sumAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);
    assertThat(sumAggBuilder.getName()).isEqualTo("1");
    assertThat(sumAggBuilder.getValueSource().getField()).isEqualTo("duration_ms");
    assertThat(sumAggBuilder.getValueSource().getScript().getStringValue()).isEqualTo("return 8;");
    assertThat(sumAggBuilder.getValueSource().getMissing().getStringValue()).isEqualTo("2");

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testDateHistogramWithNestedMovingAvg() throws IOException {
    String rawRequest = getRawQueryString("datehistogram_movavg");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getValueSource().getField())
        .isEqualTo(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    // todo - more asserts
    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);
    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);
    AstraSearch.SearchRequest.SearchAggregation movAvgAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);
    assertThat(movAvgAggBuilder.getName()).isEqualTo("3");
    assertThat(movAvgAggBuilder.getType()).isEqualTo("moving_avg");
    assertThat(movAvgAggBuilder.getPipeline().getBucketsPath()).isEqualTo("_count");

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testFilters() throws IOException {
    String rawRequest = getRawQueryString("filters");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);
    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation filtersAggregation =
        parsedRequestList.get(0).getAggregations();

    assertThat(filtersAggregation.getName()).isEqualTo("2");
    assertThat(filtersAggregation.getFilters().getFiltersCount()).isEqualTo(2);
    assertThat(filtersAggregation.getFilters().getFiltersMap().containsKey("foo")).isTrue();
    assertThat(filtersAggregation.getFilters().getFiltersMap().containsKey("bar")).isTrue();
    assertThat(filtersAggregation.getFilters().getFiltersMap().get("foo").getQueryString())
        .isEqualTo("*:*");
    assertThat(filtersAggregation.getFilters().getFiltersMap().get("foo").getAnalyzeWildcard())
        .isEqualTo(true);
    assertThat(filtersAggregation.getFilters().getFiltersMap().get("bar").getQueryString())
        .isEqualTo("*");
    assertThat(filtersAggregation.getFilters().getFiltersMap().get("bar").getAnalyzeWildcard())
        .isEqualTo(false);

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }

  @Test
  public void testTermsAggregation() throws IOException {
    String rawRequest = getRawQueryString("terms");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);
    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest.SearchAggregation rawTermsAggregation =
        parsedRequestList.get(0).getAggregations();

    AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.TermsAggregation
        typedTermsAggregation = rawTermsAggregation.getValueSource().getTerms();

    assertThat(rawTermsAggregation.getValueSource().getField()).isEqualTo("host");
    assertThat(typedTermsAggregation.getMinDocCount()).isEqualTo(1);
    assertThat(typedTermsAggregation.getSize()).isEqualTo(10);
    assertThat(typedTermsAggregation.getOrderMap().get("_term")).isEqualTo("desc");

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(parsedRequestList.getFirst().getQuery())
        .isEqualTo(parsedRequest.get("query").toString());
  }
}
