package com.slack.astra.logstore.search;

import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.aggregations.AggBuilder;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.index.query.QueryBuilder;

public class AlreadyClosedLogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  @Override
  public SearchResult<LogMessage> search(
      String dataset,
      String query,
      Long minTime,
      Long maxTime,
      int howMany,
      AggBuilder aggBuilder,
      QueryBuilder queryBuilder) {
    throw new AlreadyClosedException("Failed to acquire an index searcher");
  }

  @Override
  public void close() {
    // do nothing
  }
}
