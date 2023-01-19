package com.slack.kaldb.logstore.search;

import java.util.List;
import java.util.Map;

public class SearchAggregation {
  private String name;
  private String type;
  private Map<String, Object> metadata;

  private List<SearchAggregation> subAggregators;

  public SearchAggregation() {
    this.name = "";
    this.type = "";
    this.metadata = Map.of();
    this.subAggregators = List.of();
  }

  public SearchAggregation(
      String name,
      String type,
      Map<String, Object> metadata,
      List<SearchAggregation> subAggregators) {
    this.name = name;
    this.type = type;
    this.metadata = metadata;
    this.subAggregators = subAggregators;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  public List<SearchAggregation> getSubAggregators() {
    return subAggregators;
  }
}
