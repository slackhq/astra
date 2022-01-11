package com.slack.kaldb.elasticsearchApi.searchRequest;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Locale;

public enum SearchRequestSort {
  ASC,
  DESC;

  public static SearchRequestSort parse(JsonNode sort) {
    if (sort != null) {
      return SearchRequestSort.valueOf(sort.findValue("order").asText().toUpperCase(Locale.ROOT));
    }
    return SearchRequestSort.DESC;
  }
}
