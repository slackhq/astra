package com.slack.kaldb.logstore.search.aggregations;

/** Aggregation request type to calculate the average value */
public class AvgAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "avg";

  public AvgAggBuilder(String name, String field, Object missing) {
    super(name, field, missing);
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
