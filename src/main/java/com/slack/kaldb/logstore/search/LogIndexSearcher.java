package com.slack.kaldb.logstore.search;

public interface LogIndexSearcher<T> {
  SearchResult<T> search(
      String indexName, String query, long minTime, long maxTime, int howMany, int bucketCount);

  void close();
}
