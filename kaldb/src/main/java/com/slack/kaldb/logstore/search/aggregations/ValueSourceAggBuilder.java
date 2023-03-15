package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

/**
 * Provides common functionality for aggregations requiring operations that reference a field. This
 * would include things like avg, or date histogram, but not raw data, logs, or filters.
 */
public abstract class ValueSourceAggBuilder extends AggBuilderBase {
  // The name of the field to perform aggregations on (ie, @timestamp, duration_ms)
  protected final String field;

  // The value to use when no value is calculated
  protected final Object missing;

  public ValueSourceAggBuilder(String name, String field) {
    this(name, field, null);
  }

  public ValueSourceAggBuilder(String name, String field, Object missing) {
    this(name, Map.of(), List.of(), field, missing);
  }

  public ValueSourceAggBuilder(
      String name, Map<String, Object> metadata, List<AggBuilder> subAggregations, String field) {
    this(name, metadata, subAggregations, field, null);
  }

  public ValueSourceAggBuilder(
      String name,
      Map<String, Object> metadata,
      List<AggBuilder> subAggregations,
      String field,
      Object missing) {
    super(name, metadata, subAggregations);
    this.field = field;
    this.missing = missing;
  }

  public String getField() {
    return field;
  }

  public Object getMissing() {
    return missing;
  }
}
