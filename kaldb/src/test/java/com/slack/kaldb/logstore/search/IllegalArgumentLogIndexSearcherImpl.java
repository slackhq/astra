package com.slack.kaldb.logstore.search;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.aggregations.AggBuilder;

public class IllegalArgumentLogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  @Override
  public SearchResult<LogMessage> search(
      String dataset,
      String query,
      long minTime,
      long maxTime,
      int howMany,
      AggBuilder aggBuilder) {
    throw new IllegalArgumentException("Failed to acquire an index searcher");
  }

  @Override
  public void close() {
    // do nothing
  }
}
