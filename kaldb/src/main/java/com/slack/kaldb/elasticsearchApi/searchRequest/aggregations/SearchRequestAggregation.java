package com.slack.kaldb.elasticsearchApi.searchRequest.aggregations;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Deprecated
public abstract class SearchRequestAggregation {

  @JsonIgnore private final String aggregationKey;

  public SearchRequestAggregation(String aggregationKey) {
    this.aggregationKey = aggregationKey;
  }

  public String getAggregationKey() {
    return aggregationKey;
  }

  public static List<SearchRequestAggregation> parse(JsonNode aggs) {
    List<SearchRequestAggregation> aggregations = new ArrayList<>();

    if (aggs != null) {
      aggs.fieldNames()
          .forEachRemaining(
              aggregationKey -> {
                if (aggs.get(aggregationKey).has("date_histogram")) {
                  JsonNode node = aggs.get(aggregationKey).get("date_histogram");

                  aggregations.add(
                      new DateHistogramAggregation(
                          aggregationKey,
                          node.get("interval").asText(),
                          // min_doc_count is provided as a string in the json payload
                          Long.parseLong(node.get("min_doc_count").asText()),
                          node.has("offset") ? node.get("offset").asText() : null,
                          node.get("field").asText(),
                          node.get("format").asText(),
                          Map.of(
                              "min", node.get("extended_bounds").get("min").asLong(),
                              "max", node.get("extended_bounds").get("max").asLong())));
                }

                // todo - support other aggregation types
              });
    }

    return aggregations;
  }
}
