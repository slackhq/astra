package com.slack.astra.elasticsearchApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.slack.astra.proto.service.AstraSearch;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
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
  public void testGetAggregationJson() throws Exception {
    String rawRequest = getRawQueryString("datehistogram");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(request.getAggregationJson()).isEqualTo(parsedRequest.get("aggs").toString());
  }

  @Test
  public void testGetDateRangeFromAtTimestamp() throws Exception {
    String rawRequest = getRawQueryString("bool_query_with_@timestamp_range");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);
  }

  @Test
  public void testGetDateRangeFromStringValue() throws Exception {
    String rawRequest = getRawQueryString("bool_query_with_string_time_range");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1726766654000L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1726768454000L);
  }

  @Test
  public void testGetDateRangeFromEpochMillisStringValue() throws Exception {
    String rawRequest = getRawQueryString("bool_query_with_epoch_millis_date_range_as_string");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);
  }

  @Test
  public void testGetDateRangeFromGtLt() throws Exception {
    String rawRequest = getRawQueryString("bool_query_with_gt_lt_time_range");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1726766654000L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1726768454000L);
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
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);

    // Assert that the includes fields are set correctly
    assertThat(request.getSourceFieldFilter().getIncludeFieldsMap()).isEmpty();
    assertThat(request.getSourceFieldFilter().getIncludeWildcardsCount()).isZero();
    assertThat(request.getSourceFieldFilter().hasIncludeAll()).isTrue();
    assertThat(request.getSourceFieldFilter().getIncludeAll()).isTrue();

    // Assert that the excludes fields are set correctly
    assertThat(request.getSourceFieldFilter().getExcludeFieldsMap()).isEmpty();
    assertThat(request.getSourceFieldFilter().getExcludeWildcardsCount()).isZero();
    assertThat(request.getSourceFieldFilter().hasExcludeAll()).isFalse();

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
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);

    // Assert that the includes fields are set correctly
    assertThat(request.getSourceFieldFilter().getIncludeFieldsMap()).hasSize(1);
    assertThat(request.getSourceFieldFilter().getIncludeFieldsMap().get("normal_field_test"))
        .isTrue();
    assertThat(request.getSourceFieldFilter().getIncludeWildcardsCount()).isOne();
    assertThat(request.getSourceFieldFilter().getIncludeWildcards(0)).isEqualTo("wildcard_test.*");
    assertThat(request.getSourceFieldFilter().hasIncludeAll()).isFalse();

    // Assert that the excludes fields are set correctly
    assertThat(request.getSourceFieldFilter().getExcludeFieldsMap()).isEmpty();
    assertThat(request.getSourceFieldFilter().getExcludeWildcardsCount()).isZero();
    assertThat(request.getSourceFieldFilter().hasExcludeAll()).isFalse();

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
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);

    // Assert that the includes fields are set correctly
    assertThat(request.getSourceFieldFilter().getIncludeFieldsMap()).hasSize(1);
    assertThat(
            request.getSourceFieldFilter().getIncludeFieldsMap().get("include_normal_field_test"))
        .isTrue();
    assertThat(request.getSourceFieldFilter().getIncludeWildcardsCount()).isOne();
    assertThat(request.getSourceFieldFilter().getIncludeWildcards(0))
        .isEqualTo("include_wildcard_test.*");
    assertThat(request.getSourceFieldFilter().hasIncludeAll()).isFalse();

    // Assert that the excludes fields are set correctly
    assertThat(request.getSourceFieldFilter().getExcludeFieldsMap()).hasSize(1);
    assertThat(
            request.getSourceFieldFilter().getExcludeFieldsMap().get("exclude_normal_field_test"))
        .isTrue();
    assertThat(request.getSourceFieldFilter().getExcludeWildcardsCount()).isOne();
    assertThat(request.getSourceFieldFilter().getExcludeWildcards(0))
        .isEqualTo("exclude_wildcard_test.*");
    assertThat(request.getSourceFieldFilter().getExcludeAll()).isFalse();

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
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1676498801027L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1676500240688L);

    JsonNode parsedRequest = objectMapper.readTree(rawRequest.split("\n")[1]);
    assertThat(request.getQuery()).isEqualTo(parsedRequest.get("query").toString());
  }
}
