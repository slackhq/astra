package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

public abstract class AggBuilderBase implements AggBuilder {
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
