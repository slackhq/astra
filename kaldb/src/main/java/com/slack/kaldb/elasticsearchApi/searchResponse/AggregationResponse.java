package com.slack.kaldb.elasticsearchApi.searchResponse;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@Deprecated
public class AggregationResponse {

  @JsonProperty("buckets")
  private final List<AggregationBucketResponse> buckets;

  public AggregationResponse(List<AggregationBucketResponse> buckets) {
    this.buckets = buckets;
  }
}
