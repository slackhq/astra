package com.slack.kaldb.logstore.search.aggregations;

public class MinAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "min";

  public MinAggBuilder(String name, String field, Object missing) {
    super(name, field, missing);
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public String toString() {
    return "MinAggregationBuilder{"
        + "field='"
        + field
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
