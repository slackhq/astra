package com.slack.kaldb.elasticsearchApi.searchResponse;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AggregationBucketResponse {

  @JsonProperty("key")
  private final double key;

  @JsonProperty("doc_count")
  private final double docCount;

  public AggregationBucketResponse(double key, double docCount) {
    this.key = key;
    this.docCount = docCount;
  }

  @JsonProperty("key_as_string")
  public String getKeyAsString() {
    return String.valueOf(key);
  }
}
