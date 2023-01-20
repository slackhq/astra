package com.slack.kaldb.elasticsearchApi.searchResponse;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AggregationBucketResponse {

  @JsonProperty("key")
  private final Object key;

  @JsonProperty("doc_count")
  private final double docCount;

  public AggregationBucketResponse(Object key, double docCount) {
    this.key = key;
    this.docCount = docCount;
  }

  @JsonProperty("key_as_string")
  public String getKeyAsString() {
    return String.valueOf(key);
  }
}
