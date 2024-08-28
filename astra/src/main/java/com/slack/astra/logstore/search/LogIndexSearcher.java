package com.slack.astra.logstore.search;

import com.slack.astra.logstore.search.aggregations.AggBuilder;
import java.io.Closeable;
import org.opensearch.index.query.QueryBuilder;

public interface LogIndexSearcher<T> extends Closeable {
  SearchResult<T> search(
      String dataset,
      int howMany,
      AggBuilder aggBuilder,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter);
}
