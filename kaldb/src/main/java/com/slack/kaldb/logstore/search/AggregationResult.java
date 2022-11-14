package com.slack.kaldb.logstore.search;

import java.util.Map;

public class AggregationResult {

  final String name;
  final AggregationDefinition.AggregationType type;
  final String field;
  final double value;
  final Map<String, Object> parameters;
  final Map<String, Object> additionalResults;

  public AggregationResult(AggregationDefinition agg, double value) {
    this.name = agg.name;
    this.type = agg.type;
    this.field = agg.field;
    this.value = value;
    this.parameters = agg.parameters;
    this.additionalResults = Map.of();
  }

  public AggregationResult(
      AggregationDefinition agg, double value, Map<String, Object> additionalResults) {
    this.name = agg.name;
    this.type = agg.type;
    this.field = agg.field;
    this.value = value;
    this.parameters = agg.parameters;
    this.additionalResults = Map.copyOf(additionalResults);
  }
}
