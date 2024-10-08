package com.slack.astra.logstore.search;

import java.io.Closeable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;

public interface LogIndexSearcher<T> extends Closeable {
  SearchResult<T> search(
      String dataset,
      int howMany,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder);
}
