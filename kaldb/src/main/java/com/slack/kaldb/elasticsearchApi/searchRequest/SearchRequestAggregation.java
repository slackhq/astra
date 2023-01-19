package com.slack.kaldb.elasticsearchApi.searchRequest;

import java.util.List;
import java.util.Map;

public class SearchRequestAggregation {

  private final String name;

  private final String type;

  private final Map<String, Object> metadata;

  private final List<SearchRequestAggregation> subAggregators;

  public SearchRequestAggregation(
      String name,
      String type,
      Map<String, Object> metadata,
      List<SearchRequestAggregation> subAggregators) {
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

  public List<SearchRequestAggregation> getSubAggregators() {
    return subAggregators;
  }
}
