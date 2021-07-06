package com.slack.kaldb.elasticsearchApi.searchRequest;

import com.fasterxml.jackson.databind.JsonNode;

public class SearchRequestTimeRange {

  private final long gteEpochMillis;
  private final long lteEpochMillis;

  public SearchRequestTimeRange(long gteEpochMillis, long lteEpochMillis) {
    this.gteEpochMillis = gteEpochMillis;
    this.lteEpochMillis = lteEpochMillis;
  }

  public long getGteEpochMillis() {
    return gteEpochMillis;
  }

  public long getLteEpochMillis() {
    return lteEpochMillis;
  }

  public static SearchRequestTimeRange parse(JsonNode query) {
    return new SearchRequestTimeRange(
        query.findValue("gte").asLong(), query.findValue("lte").asLong());
  }
}
