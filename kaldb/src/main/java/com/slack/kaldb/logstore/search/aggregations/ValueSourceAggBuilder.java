package com.slack.kaldb.logstore.search.aggregations;

public abstract class ValueSourceAggBuilder extends AggBuilderBase {
  private final String field;

  public ValueSourceAggBuilder(String name, String field) {
    super(name);
    this.field = field;
  }

  public String getField() {
    return field;
  }
}
