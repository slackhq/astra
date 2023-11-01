package com.slack.kaldb.elasticsearchApi.searchResponse;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;

public class EsSearchResponse {

  @JsonProperty("took")
  private final long took;

  @JsonProperty("timed_out")
  private final boolean timedOut;

  @JsonProperty("_shards")
  private final Map<String, Integer> shardsMetadata;

  @JsonProperty("_debug")
  @SuppressWarnings("unused")
  private final Map<String, String> debugMetadata;

  @JsonProperty("hits")
  private final HitsMetadata hitsMetadata;

  @JsonProperty("aggregations")
  private final JsonNode aggregations;

  @JsonProperty("status")
  private final int status;

  public EsSearchResponse(
      long took,
      boolean timedOut,
      Map<String, Integer> shardsMetadata,
      Map<String, String> debugMetadata,
      HitsMetadata hitsMetadata,
      JsonNode aggregations,
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

  public JsonNode getAggregations() {
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
    private JsonNode aggregations;
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

    public Builder aggregations(JsonNode aggregations) {
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
