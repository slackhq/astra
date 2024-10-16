package com.slack.astra.logstore.search;

import com.slack.astra.logstore.LogMessage;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;

public class IllegalArgumentLogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  @Override
  public SearchResult<LogMessage> search(
      String dataset,
      int howMany,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    throw new IllegalArgumentException("Failed to acquire an index searcher");
  }

  @Override
  public void close() {
    // do nothing
  }
}
