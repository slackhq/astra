package com.slack.astra.logstore.search;

import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.aggregations.AggBuilder;
import com.slack.astra.proto.service.AstraSearch;
import org.opensearch.index.query.QueryBuilder;

public class IllegalArgumentLogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  @Override
  public SearchResult<LogMessage> search(
      String dataset,
      String query,
      Long minTime,
      Long maxTime,
      int howMany,
      AggBuilder aggBuilder,
      QueryBuilder queryBuilder,
      AstraSearch.SearchRequest.FieldInclusion includeFields,
      AstraSearch.SearchRequest.FieldInclusion excludeFields) {
    throw new IllegalArgumentException("Failed to acquire an index searcher");
  }

  @Override
  public void close() {
    // do nothing
  }
}
