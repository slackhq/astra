package com.slack.astra.elasticsearchApi.searchResponse;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class SearchResponseMetadata {

  @JsonProperty("took")
  private final long took;

  @JsonProperty("responses")
  private final List<EsSearchResponse> responses;

  @JsonProperty("_debug")
  private final Map<String, String> debugMetadata;

  public SearchResponseMetadata(
      long took, List<EsSearchResponse> responses, Map<String, String> debugMetadata) {
    this.took = took;
    this.responses = responses;
    this.debugMetadata = debugMetadata;
  }

  public long getTook() {
    return took;
  }

  public List<EsSearchResponse> getResponses() {
    return responses;
  }

  public Map<String, String> getDebugMetadata() {
    return debugMetadata;
  }
}
