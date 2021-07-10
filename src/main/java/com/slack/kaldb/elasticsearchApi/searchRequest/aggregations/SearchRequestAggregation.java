package com.slack.kaldb.elasticsearchApi.searchRequest.aggregations;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;

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
                          node.get("min_doc_count").asInt()));
                }

                // todo - support other aggregation types
              });
    }

    return aggregations;
  }
}
