package com.slack.kaldb.logstore.search.aggregations;

/** Aggregation request type to calculate the average value */
public class AvgAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "avg";

  public AvgAggBuilder(String name, String field) {
    super(name, field);
  }

  @Override
  public String getType() {
    return TYPE;
  }
}
