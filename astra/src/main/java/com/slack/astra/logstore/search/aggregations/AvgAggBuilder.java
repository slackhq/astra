package com.slack.astra.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

/** Aggregation request type to calculate the average value */
public class AvgAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "avg";

  public AvgAggBuilder(String name, String field, Object missing, String script) {
    super(name, Map.of(), List.of(), field, missing, script);
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public String toString() {
    return "AvgAggBuilder{"
        + "field='"
        + field
        + '\''
        + ", missing="
        + missing
        + ", script='"
        + script
        + '\''
        + ", name='"
        + name
        + '\''
        + ", metadata="
        + metadata
        + ", subAggregations="
        + subAggregations
        + '}';
  }
}
