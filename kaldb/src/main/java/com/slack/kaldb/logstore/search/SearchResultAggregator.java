package com.slack.kaldb.logstore.search;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SearchResultAggregator<T> {
  CompletableFuture<SearchResult<T>> aggregate(
      CompletableFuture<List<SearchResult<T>>> searchResults);

  SearchResult<T> aggregate(List<SearchResult<T>> searchResults);
}
