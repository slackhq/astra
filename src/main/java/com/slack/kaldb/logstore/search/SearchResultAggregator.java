package com.slack.kaldb.logstore.search;

import java.util.List;

public interface SearchResultAggregator<T> {
  SearchResult<T> aggregate(List<SearchResult<T>> searchResults, SearchQuery query);
}
