package com.slack.kaldb.logstore.search;

import com.slack.kaldb.logstore.LogMessage;
import org.apache.lucene.store.AlreadyClosedException;

public class AlreadyClosedLogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  @Override
  public SearchResult<LogMessage> search(
      String dataset,
      String query,
      long minTime,
      long maxTime,
      int howMany,
      SearchAggregation searchAggregation) {
    throw new AlreadyClosedException("Failed to acquire an index searcher");
  }

  @Override
  public void close() {
    // do nothing
  }
}
