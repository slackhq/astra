package com.slack.kaldb.elasticsearchApi.searchRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.slack.kaldb.elasticsearchApi.searchRequest.aggregations.DateHistogramAggregation;
import com.slack.kaldb.elasticsearchApi.searchRequest.aggregations.SearchRequestAggregation;
import com.slack.kaldb.proto.service.KaldbSearch;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsSearchRequest {
  private static final Logger LOG = LoggerFactory.getLogger(EsSearchRequest.class);

  private final String index;
  private final int size;
  private final String query;
  private final SearchRequestTimeRange range;
  private final SearchRequestSort sort;
  private final List<SearchRequestAggregation> aggregations;

  private EsSearchRequest(
      String index,
      int size,
      String query,
      SearchRequestTimeRange range,
      SearchRequestSort sort,
      List<SearchRequestAggregation> aggregations) {
    this.index = index;
    this.size = size;
    this.query = query;
    this.range = range;
    this.sort = sort;
    this.aggregations = aggregations;
  }

  public String getIndex() {
    return index;
  }

  public int getSize() {
    return size;
  }

  public String getQuery() {
    return query;
  }

  public SearchRequestTimeRange getRange() {
    return range;
  }

  public SearchRequestSort getSort() {
    return sort;
  }

  public List<SearchRequestAggregation> getAggregations() {
    return aggregations;
  }

  /**
   * This is a temporary fix for calculating the buckets required in a date histogram. This code
   * should potentially exist somewhere else, or an entirely different approach should be
   * considered. This will have unexpected behavior when multiple aggregations are provided. As a
   * larger point the bucket count is specific to a date histogram, and if other aggregations are
   * requested this field makes no sense (ie, terms query). A larger refactor of the
   * KaldbSearch.SearchRequest object is needed to appropriate resolve this.
   */
  @Deprecated
  protected static int getBucketCount(
      List<SearchRequestAggregation> searchRequestAggregations, SearchRequestTimeRange timeRange) {
    String intervalString = "";
    try {
      DateHistogramAggregation dateHistogramAggregation =
          (DateHistogramAggregation) searchRequestAggregations.get(0);
      intervalString = dateHistogramAggregation.getInterval();

      // ISO-8601 duration spec requires different input format for days than hours/mins/seconds
      String durationFormat = "PT%s";
      if (intervalString.endsWith("d")) {
        durationFormat = "P%s";
      }

      Duration intervalDuration =
          Duration.parse(String.format(durationFormat, intervalString.toUpperCase()));
      return Ints.saturatedCast(
          (timeRange.getLteEpochMillis() - timeRange.getGteEpochMillis())
              / intervalDuration.toMillis());
    } catch (Exception e) {
      // for any issue parsing or calculating the input, just log it and default to 60
      LOG.warn(
          "Error converting user input intervalString:'{}', defaulting to 60 buckets",
          intervalString);
      return 60;
    }
  }

  public KaldbSearch.SearchRequest toKaldbSearchRequest() {
    return KaldbSearch.SearchRequest.newBuilder()
        .setDataset(getIndex())
        .setQueryString(getQuery())
        .setStartTimeEpochMs(getRange().getGteEpochMillis())
        .setEndTimeEpochMs(getRange().getLteEpochMillis())
        .setHowMany(getSize())
        .setBucketCount(getBucketCount(getAggregations(), getRange()))
        .build();
  }

  public static List<EsSearchRequest> parse(String postBody) throws JsonProcessingException {
    ObjectMapper om =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // the body contains an NDJSON format, with alternating rows as header/body
    // @see http://ndjson.org/
    // @see
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html#search-multi-search-api-desc
    List<EsSearchRequest> requests = new ArrayList<>();
    for (List<String> pair : Lists.partition(Arrays.asList(postBody.split("\n")), 2)) {
      JsonNode header = om.readTree(pair.get(0));
      JsonNode body = om.readTree(pair.get(1));

      // Grafana 7 and 8 have different default behaviors when query is not initialized
      // - Grafana 7 the query field under query is not present
      // - Grafana 8 the query field defaults to "*"
      String queryString = "*:*";
      if (body.get("query").findValue("query") != null) {
        String requestedQueryString = body.get("query").findValue("query").asText();
        if (!requestedQueryString.equals("*")) {
          queryString = requestedQueryString;
        }
      }
      requests.add(
          new EsSearchRequest(
              header.get("index").asText(),
              body.get("size").asInt(),
              queryString,
              SearchRequestTimeRange.parse(body.get("query")),
              SearchRequestSort.parse(body.get("sort")),
              SearchRequestAggregation.parse(body.get("aggs"))));
    }
    return requests;
  }
}
