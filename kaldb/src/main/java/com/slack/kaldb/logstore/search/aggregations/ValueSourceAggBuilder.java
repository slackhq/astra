package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

public abstract class ValueSourceAggBuilder extends AggBuilderBase {
  private final String field;

  public ValueSourceAggBuilder(String name, String field) {
    super(name);
    this.field = field;
  }

  public ValueSourceAggBuilder(
      String name, Map<String, Object> metadata, List<AggBuilder> subAggregations, String field) {
    super(name, metadata, subAggregations);
    this.field = field;
  }

  public String getField() {
    return field;
  }
}
