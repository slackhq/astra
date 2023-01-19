package com.slack.kaldb.elasticsearchApi.searchRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.slack.kaldb.proto.service.KaldbSearch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

  public KaldbSearch.SearchRequest toKaldbSearchRequest() {
    KaldbSearch.SearchAggregation aggregation = searchAggregation(aggregations).get(0);
    return KaldbSearch.SearchRequest.newBuilder()
        .setDataset(getIndex())
        .setQueryString(getQuery())
        .setStartTimeEpochMs(getRange().getGteEpochMillis())
        .setEndTimeEpochMs(getRange().getLteEpochMillis())
        .setHowMany(getSize())
        .setAggs(aggregation)
        .build();
  }

  private List<KaldbSearch.SearchAggregation> searchAggregation(
      List<SearchRequestAggregation> searchRequestAggregations) {
    List<KaldbSearch.SearchAggregation> returnList = new ArrayList<>();
    searchRequestAggregations.forEach(
        searchRequestAggregation -> {
          returnList.add(
              KaldbSearch.SearchAggregation.newBuilder()
                  .setName(searchRequestAggregation.getName())
                  .setType(searchRequestAggregation.getType())
                  .setMetadata(mapToProtoStruct(searchRequestAggregation.getMetadata()))
                  .addAllSubAggregators(
                      searchAggregation(searchRequestAggregation.getSubAggregators()))
                  .build());
        });
    return returnList;
  }

  private KaldbSearch.Value objectToProtoValue(Object value) {
    KaldbSearch.Value.Builder valueBuilder = KaldbSearch.Value.newBuilder();
    if (value instanceof Boolean) {
      valueBuilder.setBoolValue((Boolean) value);
    } else if (value instanceof Double) {
      valueBuilder.setDoubleValue((Double) value);
    } else if (value instanceof Long) {
      valueBuilder.setIntValue((Long) value);
    } else if (value instanceof Integer) {
      valueBuilder.setIntValue((Integer) value);
    } else if (value instanceof String) {
      valueBuilder.setStringValue((String) value);
    } else if (value instanceof Map) {
      valueBuilder.setStructValue(mapToProtoStruct((Map<String, Object>) value));
    } else if (value instanceof List) {
      valueBuilder.setListValue(listToListValueStruct((List<Object>) value));
    } else {
      throw new IllegalArgumentException();
    }

    return valueBuilder.build();
  }

  private KaldbSearch.ListValue listToListValueStruct(List<Object> list) {
    KaldbSearch.ListValue.Builder builder = KaldbSearch.ListValue.newBuilder();
    list.forEach(listElement -> builder.addValues(objectToProtoValue(listElement)));
    return builder.build();
  }

  private KaldbSearch.Struct mapToProtoStruct(Map<String, Object> searchRequestMetadata) {
    KaldbSearch.Struct.Builder builder = KaldbSearch.Struct.newBuilder();
    searchRequestMetadata.forEach(
        (key, value) -> builder.putFields(key, objectToProtoValue(value)));
    return builder.build();
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
              getSearchRequestAggs(body.get("aggs"))));
    }
    return requests;
  }

  private static List<SearchRequestAggregation> getSearchRequestAggs(JsonNode aggsNode) {
    ObjectMapper om = new ObjectMapper();

    List<SearchRequestAggregation> returnAggregators = new ArrayList<>();
    aggsNode
        .fields()
        .forEachRemaining(
            field -> {
              List<SearchRequestAggregation> nestedAggregators = new ArrayList<>();
              List<SearchRequestAggregation> aggregator = new ArrayList<>();
              field
                  .getValue()
                  .fields()
                  .forEachRemaining(
                      nestedField -> {
                        if (nestedField.getKey().equals("aggs")) {
                          nestedAggregators.addAll(getSearchRequestAggs(nestedField.getValue()));
                        } else {
                          // ie, max, min
                          aggregator.add(
                              new SearchRequestAggregation(
                                  field.getKey(),
                                  nestedField.getKey(),
                                  om.convertValue(nestedField.getValue(), Map.class),
                                  nestedAggregators));
                        }
                      });
              returnAggregators.addAll(aggregator);
            });
    return returnAggregators;
  }
}
