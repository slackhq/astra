package com.slack.astra.logstore.search;

import com.slack.astra.logstore.search.aggregations.AggBuilder;
import com.slack.astra.proto.service.AstraSearch;
import java.io.Closeable;
import org.opensearch.index.query.QueryBuilder;

public interface LogIndexSearcher<T> extends Closeable {
  SearchResult<T> search(
      String dataset,
      String query,
      Long minTime,
      Long maxTime,
      int howMany,
      AggBuilder aggBuilder,
      QueryBuilder queryBuilder,
      AstraSearch.SearchRequest.FieldInclusion includeFields,
      AstraSearch.SearchRequest.FieldInclusion excludeFields);
}
