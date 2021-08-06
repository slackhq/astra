package com.slack.kaldb.elasticsearchApi.searchResponse;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class SearchResponseMetadata {

  @JsonProperty("took")
  private final long took;

  @JsonProperty("responses")
  private final List<EsSearchResponse> responses;

  public SearchResponseMetadata(long took, List<EsSearchResponse> responses) {
    this.took = took;
    this.responses = responses;
  }

  public long getTook() {
    return took;
  }

  public List<EsSearchResponse> getResponses() {
    return responses;
  }
}
