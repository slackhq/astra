package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

/**
 * Provides common functionality for aggregations requiring operations that reference a field. This
 * would include things like avg, or date histogram, but not raw data, logs, or filters.
 */
public abstract class ValueSourceAggBuilder extends AggBuilderBase {
  // The name of the field to perform aggregations on (ie, @timestamp, duration_ms)
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
