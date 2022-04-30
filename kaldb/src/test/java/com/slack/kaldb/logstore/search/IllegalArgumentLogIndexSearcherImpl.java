package com.slack.kaldb.logstore.search;

import com.slack.kaldb.logstore.LogMessage;

public class IllegalArgumentLogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  @Override
  public SearchResult<LogMessage> search(
      String indexName, String query, long minTime, long maxTime, int howMany, int bucketCount) {
    throw new IllegalArgumentException("Failed to acquire an index searcher");
  }

  @Override
  public void close() {
    // do nothing
  }
}
