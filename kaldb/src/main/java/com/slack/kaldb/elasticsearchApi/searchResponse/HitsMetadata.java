package com.slack.kaldb.elasticsearchApi.searchResponse;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HitsMetadata {

  @JsonProperty("total")
  private final Map<String, Object> hitsTotal;

  @JsonProperty("max_score")
  private final String maxScore;

  @JsonProperty("hits")
  private final List<SearchResponseHit> hits;

  public HitsMetadata(
      Map<String, Object> hitsTotal, String maxScore, List<SearchResponseHit> hits) {
    this.hitsTotal = hitsTotal;
    this.maxScore = maxScore;
    this.hits = hits;
  }

  public Map<String, Object> getHitsTotal() {
    return hitsTotal;
  }

  public String getMaxScore() {
    return maxScore;
  }

  public List<SearchResponseHit> getHits() {
    return hits;
  }

  public static class Builder {
    private Map<String, Object> hitsTotal;
    private String maxScore;
    private List<SearchResponseHit> hits = new ArrayList<>();

    public Builder hitsTotal(Map<String, Object> hitsTotal) {
      this.hitsTotal = hitsTotal;
      return this;
    }

    public Builder maxScore(String maxScore) {
      this.maxScore = maxScore;
      return this;
    }

    public Builder hits(List<SearchResponseHit> hits) {
      this.hits = hits;
      return this;
    }

    public HitsMetadata build() {
      return new HitsMetadata(hitsTotal, maxScore, hits);
    }
  }
}
