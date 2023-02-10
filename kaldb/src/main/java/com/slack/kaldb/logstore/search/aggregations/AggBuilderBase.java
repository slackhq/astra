package com.slack.kaldb.logstore.search.aggregations;

import java.util.Map;

public abstract class AggBuilderBase implements AggBuilder {
  protected final String name;
  protected final Map<String, Object> metadata = Map.of();

  public AggBuilderBase(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }
}
