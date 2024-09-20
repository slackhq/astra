package com.slack.astra.logstore.opensearch;

import static java.util.Collections.emptyMap;

import org.opensearch.index.similarity.SimilarityService;

public class AstraSimilarityService {
  private static SimilarityService similarityService = null;

  private AstraSimilarityService() {}

  public static SimilarityService getInstance() {
    if (similarityService == null) {
      similarityService = new SimilarityService(AstraIndexSettings.getInstance(), null, emptyMap());
    }
    return similarityService;
  }
}
