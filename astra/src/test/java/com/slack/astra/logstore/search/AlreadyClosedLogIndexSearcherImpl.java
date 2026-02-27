package com.slack.astra.logstore.search;

import com.slack.astra.logstore.LogMessage;
import java.util.List;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;

public class AlreadyClosedLogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  @Override
  public SearchResult<LogMessage> search(
      String dataset,
      int howMany,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder,
      List<SearchQuery.SortSpec> sortFields) {
    throw new AlreadyClosedException("Failed to acquire an index searcher");
  }

  @Override
  public void close() {
    // do nothing
  }
}
