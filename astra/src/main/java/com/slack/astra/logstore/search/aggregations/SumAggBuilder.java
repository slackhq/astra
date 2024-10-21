package com.slack.astra.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

public class SumAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "sum";

  public SumAggBuilder(String name, String field, Object missing, String script) {
    super(name, Map.of(), List.of(), field, missing, script);
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public String toString() {
    return "SumAggBuilder{"
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
