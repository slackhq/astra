package com.slack.kaldb.elasticsearchApi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.slack.kaldb.logstore.search.SearchResultUtils;
import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.TermsAggBuilder;
import com.slack.kaldb.proto.service.KaldbSearch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;

/**
 * Utility class for parsing an OpenSearch NDJSON search request into a list of appropriate
 * KaldbSearch.SearchRequests, that can be provided to the GRPC Search API. This class is
 * responsible for taking a raw payload string, performing any validation as appropriate, and
 * building a complete working list of queries to be performed.
 */
public class OpenSearchRequest {
  private static final ObjectMapper OM =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public List<KaldbSearch.SearchRequest> parseHttpPostBody(String postBody)
      throws JsonProcessingException {
    // the body contains an NDJSON format, with alternating rows as header/body
    // @see http://ndjson.org/
    // @see
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html#search-multi-search-api-desc

    List<KaldbSearch.SearchRequest> searchRequests = new ArrayList<>();

    // List<EsSearchRequest> requests = new ArrayList<>();
    for (List<String> pair : Lists.partition(Arrays.asList(postBody.split("\n")), 2)) {
      JsonNode header = OM.readTree(pair.get(0));
      JsonNode body = OM.readTree(pair.get(1));

      searchRequests.add(
          KaldbSearch.SearchRequest.newBuilder()
              .setDataset(getDataset(header))
              .setQueryString(getQueryString(body))
              .setHowMany(getHowMany(body))
              .setStartTimeEpochMs(getStartTimeEpochMs(body))
              .setEndTimeEpochMs(getEndTimeEpochMs(body))
              .setAggregations(getAggregations(body))
              .build());
    }
    return searchRequests;
  }

  private static String getDataset(JsonNode header) {
    return header.get("index").asText();
  }

  private static String getQueryString(JsonNode body) {
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
    return queryString;
  }

  private static int getHowMany(JsonNode body) {
    return body.get("size").asInt();
  }

  private static long getStartTimeEpochMs(JsonNode body) {
    return body.get("query").findValue("gte").asLong();
  }

  private static long getEndTimeEpochMs(JsonNode body) {
    return body.get("query").findValue("lte").asLong();
  }

  private static KaldbSearch.SearchRequest.SearchAggregation getAggregations(JsonNode body) {
    if (Iterators.size(body.get("aggs").fieldNames()) != 1) {
      throw new NotImplementedException(
          "Only exactly one top level aggregators is currently supported");
    }
    return getRecursive(body.get("aggs")).get(0);
  }

  private static List<KaldbSearch.SearchRequest.SearchAggregation> getRecursive(JsonNode aggs) {
    List<KaldbSearch.SearchRequest.SearchAggregation> aggregations = new ArrayList<>();

    aggs.fieldNames()
        .forEachRemaining(
            aggregationName -> {
              KaldbSearch.SearchRequest.SearchAggregation.Builder aggBuilder =
                  KaldbSearch.SearchRequest.SearchAggregation.newBuilder();
              aggs.get(aggregationName)
                  .fieldNames()
                  .forEachRemaining(
                      aggregationObject -> {
                        if (aggregationObject.equals(DateHistogramAggBuilder.TYPE)) {
                          JsonNode dateHistogram = aggs.get(aggregationName).get(aggregationObject);
                          aggBuilder
                              .setType(DateHistogramAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(dateHistogram))
                                      .setDateHistogram(
                                          KaldbSearch.SearchRequest.SearchAggregation
                                              .ValueSourceAggregation.DateHistogramAggregation
                                              .newBuilder()
                                              .setMinDocCount(
                                                  getDateHistogramMinDocCount(dateHistogram))
                                              .setInterval(getDateHistogramInterval(dateHistogram))
                                              .putAllExtendedBounds(
                                                  getDateHistogramExtendedBounds(dateHistogram))
                                              .setFormat(getDateHistogramFormat(dateHistogram))
                                              .setOffset(getDateHistogramOffset(dateHistogram))
                                              .build())
                                      .build());
                        } else if (aggregationObject.equals(TermsAggBuilder.TYPE)) {
                          JsonNode terms = aggs.get(aggregationName).get(aggregationObject);
                          aggBuilder
                              .setType(TermsAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(terms))
                                      .setMissing(SearchResultUtils.toValueProto(getMissing(terms)))
                                      .setTerms(
                                          KaldbSearch.SearchRequest.SearchAggregation
                                              .ValueSourceAggregation.TermsAggregation.newBuilder()
                                              .setSize(getSize(terms))
                                              .putAllOrder(getTermsOrder(terms))
                                              .build())
                                      .build());
                        } else if (aggregationObject.equals(AvgAggBuilder.TYPE)) {
                          JsonNode avg = aggs.get(aggregationName).get(aggregationObject);
                          aggBuilder
                              .setType(AvgAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(avg))
                                      .setMissing(SearchResultUtils.toValueProto(getMissing(avg)))
                                      .build());
                        } else if (aggregationObject.equals("aggs")) {
                          // nested aggregations
                          aggBuilder.addAllSubAggregations(
                              getRecursive(aggs.get(aggregationName).get(aggregationObject)));
                        } else {
                          throw new NotImplementedException(
                              String.format(
                                  "Aggregation type '%s' is not yet supported", aggregationObject));
                        }
                      });
              aggregations.add(aggBuilder.build());
            });

    return aggregations;
  }

  private static String getDateHistogramInterval(JsonNode dateHistogram) {
    return dateHistogram.get("interval").asText();
  }

  private static String getFieldName(JsonNode agg) {
    return agg.get("field").asText();
  }

  private static Object getMissing(JsonNode agg) {
    // we can return any object here and it will correctly serialize, but Grafana only ever seems to
    // issue these as strings
    if (agg.has("missing")) {
      return agg.get("missing").asText();
    }
    return null;
  }

  private static long getDateHistogramMinDocCount(JsonNode dateHistogram) {
    // min_doc_count is provided as a string in the json payload
    return Long.parseLong(dateHistogram.get("min_doc_count").asText());
  }

  private static Map<String, Long> getDateHistogramExtendedBounds(JsonNode dateHistogram) {
    if (dateHistogram.has("extended_bounds")
        && dateHistogram.get("extended_bounds").has("min")
        && dateHistogram.get("extended_bounds").has("max")) {
      return Map.of(
          "min", dateHistogram.get("extended_bounds").get("min").asLong(),
          "max", dateHistogram.get("extended_bounds").get("max").asLong());
    }
    return Map.of();
  }

  private static String getDateHistogramFormat(JsonNode dateHistogram) {
    return dateHistogram.get("format").asText();
  }

  private static String getDateHistogramOffset(JsonNode dateHistogram) {
    if (dateHistogram.has("offset")) {
      return dateHistogram.get("offset").asText();
    }
    return "";
  }

  private static int getSize(JsonNode agg) {
    return agg.get("size").asInt();
  }

  private static Map<String, String> getTermsOrder(JsonNode terms) {
    Map<String, String> orderMap = new HashMap<>();
    JsonNode order = terms.get("order");
    order
        .fieldNames()
        .forEachRemaining(fieldName -> orderMap.put(fieldName, order.get(fieldName).asText()));
    return orderMap;
  }
}
