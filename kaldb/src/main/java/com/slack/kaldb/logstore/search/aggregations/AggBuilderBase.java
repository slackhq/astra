package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

/**
 * Provides common functionality for all aggregations. All aggregations are expected to extend this
 * class, or another base class that extends this one (ie, ValueSourceAggBuilder).
 */
public abstract class AggBuilderBase implements AggBuilder {
  // This is the name of the aggregation, as provided by the user. In most cases this will be a
  // unique string ID to correlate the output values - Grafana generally uses incrementing
  // integers as a string (ie "1", "2").
  protected final String name;
  protected final Map<String, Object> metadata;
  protected final List<AggBuilder> subAggregations;

  public AggBuilderBase(String name) {
    this.name = name;
    this.metadata = Map.of();
    this.subAggregations = List.of();
  }

  public AggBuilderBase(
      String name, Map<String, Object> metadata, List<AggBuilder> subAggregations) {
    this.name = name;
    this.metadata = metadata;
    this.subAggregations = subAggregations;
  }

  public List<AggBuilder> getSubAggregations() {
    return subAggregations;
  }

  public String getName() {
    return name;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }
}
