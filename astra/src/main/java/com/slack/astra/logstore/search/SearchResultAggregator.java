package com.slack.astra.logstore.search;

import java.util.List;

public interface SearchResultAggregator<T> {

  /**
   * Combines a list of search results into a single search result
   *
   * @param searchResults List of search results to combine
   * @param finalAggregation Marks if this is the final aggregation to be performed before returning
   *     a result
   */
  SearchResult<T> aggregate(List<SearchResult<T>> searchResults, boolean finalAggregation);
}
