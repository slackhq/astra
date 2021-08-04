package com.slack.kaldb.elasticsearchApi.searchRequest;

import com.fasterxml.jackson.databind.JsonNode;

public enum SearchRequestSort {
  ASC,
  DESC;

  public static SearchRequestSort parse(JsonNode sort) {
    if (sort != null) {
      return SearchRequestSort.valueOf(sort.findValue("order").asText().toUpperCase());
    }
    return SearchRequestSort.DESC;
  }
}
