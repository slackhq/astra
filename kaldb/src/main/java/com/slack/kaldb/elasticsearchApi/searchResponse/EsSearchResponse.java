package com.slack.kaldb.elasticsearchApi.searchResponse;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsSearchResponse {

  @JsonProperty("took")
  private final long took;

  @JsonProperty("timed_out")
  private final boolean timedOut;

  @JsonProperty("_shards")
  private final Map<String, Integer> shardsMetadata;

  @JsonProperty("_debug")
  private final Map<String, String> debugMetadata;

  @JsonProperty("hits")
  private final HitsMetadata hitsMetadata;

  @JsonProperty("aggregations")
  private final Map<String, AggregationResponse> aggregations;

  @JsonProperty("status")
  private final int status;

  public EsSearchResponse(
      long took,
      boolean timedOut,
      Map<String, Integer> shardsMetadata,
      Map<String, String> debugMetadata,
      HitsMetadata hitsMetadata,
      Map<String, AggregationResponse> aggregations,
      int status) {
    this.took = took;
    this.timedOut = timedOut;
    this.shardsMetadata = shardsMetadata;
    this.debugMetadata = debugMetadata;
    this.hitsMetadata = hitsMetadata;
    this.aggregations = aggregations;
    this.status = status;
  }

  public long getTook() {
    return took;
  }

  public boolean isTimedOut() {
    return timedOut;
  }

  public Map<String, Integer> getShardsMetadata() {
    return shardsMetadata;
  }

  public HitsMetadata getHitsMetadata() {
    return hitsMetadata;
  }

  public Map<String, AggregationResponse> getAggregations() {
    return aggregations;
  }

  public int getStatus() {
    return status;
  }

  public static class Builder {
    private long took;
    private boolean timedOut;
    private Map<String, Integer> shardsMetadata = new HashMap<>();
    private Map<String, String> debugMetadata = new HashMap<>();
    private HitsMetadata hitsMetadata;
    private Map<String, AggregationResponse> aggregations = new HashMap<>();
    private int status;

    public Builder took(long took) {
      this.took = took;
      return this;
    }

    public Builder timedOut(boolean timedOut) {
      this.timedOut = timedOut;
      return this;
    }

    public Builder shardsMetadata(int total, int failed) {
      this.shardsMetadata =
          Map.of(
              "total", total,
              "failed", failed);
      return this;
    }

    public Builder debugMetadata(Map<String, String> debugMetadata) {
      this.debugMetadata = debugMetadata;
      return this;
    }

    public Builder hits(HitsMetadata hitsMetadata) {
      this.hitsMetadata = hitsMetadata;
      return this;
    }

    public Builder aggregation(
        String key, List<AggregationBucketResponse> aggregationBucketResponse) {
      this.aggregations.put(key, new AggregationResponse(aggregationBucketResponse));
      return this;
    }

    public Builder aggregations(Map<String, AggregationResponse> aggregations) {
      this.aggregations = aggregations;
      return this;
    }

    public Builder status(int status) {
      this.status = status;
      return this;
    }

    public EsSearchResponse build() {
      return new EsSearchResponse(
          this.took,
          this.timedOut,
          this.shardsMetadata,
          this.debugMetadata,
          this.hitsMetadata,
          this.aggregations,
          this.status);
    }
  }
}
