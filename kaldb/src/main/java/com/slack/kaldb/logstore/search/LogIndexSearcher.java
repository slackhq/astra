package com.slack.kaldb.logstore.search;

import java.io.Closeable;

public interface LogIndexSearcher<T> extends Closeable {
  SearchResult<T> search(
      String dataset,
      String query,
      long minTime,
      long maxTime,
      int howMany,
      SearchAggregation searchAggregation);
}
