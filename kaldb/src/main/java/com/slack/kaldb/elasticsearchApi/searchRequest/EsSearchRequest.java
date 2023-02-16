package com.slack.kaldb.elasticsearchApi.searchRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.slack.kaldb.elasticsearchApi.searchRequest.aggregations.DateHistogramAggregation;
import com.slack.kaldb.elasticsearchApi.searchRequest.aggregations.SearchRequestAggregation;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchResultUtils;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.proto.service.KaldbSearch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;

@Deprecated
public class EsSearchRequest {
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

  /** Converts an EsSearchResult to a KaldbSearch.SearchRequest. */
  public KaldbSearch.SearchRequest toKaldbSearchRequest() {
    if (aggregations.size() > 1) {
      // only a single top-level aggregation is supported
      throw new NotImplementedException();
    }

    KaldbSearch.SearchRequest.SearchAggregation aggregation =
        KaldbSearch.SearchRequest.SearchAggregation.newBuilder().build();

    if (aggregations.size() == 1) {
      DateHistogramAggregation legacyAggRequest = (DateHistogramAggregation) aggregations.get(0);

      // todo - this is due to some incorrect indexing / schema changes
      String fieldname = legacyAggRequest.getFieldName();
      if (fieldname.equals("@timestamp")) {
        fieldname = LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName;
      }

      aggregation =
          SearchResultUtils.toSearchAggregationProto(
              new DateHistogramAggBuilder(
                  legacyAggRequest.getAggregationKey(),
                  fieldname,
                  legacyAggRequest.getInterval(),
                  legacyAggRequest.getOffset(),
                  legacyAggRequest.getMinDocCount(),
                  legacyAggRequest.getFormat(),
                  legacyAggRequest.getExtendedBounds(),
                  List.of()));
    }

    return KaldbSearch.SearchRequest.newBuilder()
        .setDataset(getIndex())
        .setQueryString(getQuery())
        .setStartTimeEpochMs(getRange().getGteEpochMillis())
        .setEndTimeEpochMs(getRange().getLteEpochMillis())
        .setHowMany(getSize())
        .setAggregations(aggregation)
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
