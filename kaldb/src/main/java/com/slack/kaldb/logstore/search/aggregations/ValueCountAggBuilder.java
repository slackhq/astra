package com.slack.kaldb.logstore.search.aggregations;

public class ValueCountAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "value_count";

  public ValueCountAggBuilder(String name, String field) {
    super(name, field);
  }

  @Override
  public String getType() {
    return TYPE;
  }
}
