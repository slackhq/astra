package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.slack.kaldb.histogram.FixedIntervalHistogramImpl;
import com.slack.kaldb.histogram.Histogram;
import com.slack.kaldb.logstore.LogMessage;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This class will merge multiple search results into a single search result. Takes all the hits
 * from all the search results and returns the topK most recent results. The histogram will be
 * merged using the histogram merge function.
 */
public class SearchResultAggregatorImpl<T extends LogMessage> implements SearchResultAggregator<T> {

  private final SearchQuery searchQuery;

  public SearchResultAggregatorImpl(SearchQuery searchQuery) {
    this.searchQuery = searchQuery;
  }

  @Override
  public CompletableFuture<SearchResult<T>> aggregate(
      CompletableFuture<List<SearchResult<T>>> searchResults) {
    return searchResults.thenApply(this::aggregate);
  }

  private SearchResult<T> aggregate(List<SearchResult<T>> searchResults) {
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("SearchResultAggregatorImpl.aggregate");
    span.tag("threadName", Thread.currentThread().getName());
    long tookMicros = 0;
    int failedNodes = 0;
    int totalNodes = 0;
    int totalSnapshots = 0;
    int snapshpotReplicas = 0;
    int totalCount = 0;
    Optional<Histogram> histogram =
        searchQuery.bucketCount > 0
            ? Optional.of(
                new FixedIntervalHistogramImpl(
                    searchQuery.startTimeEpochMs,
                    searchQuery.endTimeEpochMs,
                    searchQuery.bucketCount))
            : Optional.empty();

    for (SearchResult<T> searchResult : searchResults) {
      tookMicros = Math.max(tookMicros, searchResult.tookMicros);
      failedNodes += searchResult.failedNodes;
      totalNodes += searchResult.totalNodes;
      totalSnapshots += searchResult.totalSnapshots;
      snapshpotReplicas += searchResult.snapshotsWithReplicas;
      totalCount += searchResult.totalCount;
      histogram.ifPresent(value -> value.mergeHistogram(searchResult.buckets));
    }

    // TODO: Instead of sorting all hits using a bounded priority queue of size k is more efficient.
    List<T> resultHits =
        searchResults
            .stream()
            .flatMap(r -> r.hits.stream())
            .sorted(Comparator.comparing((T m) -> m.timeSinceEpochMilli, Comparator.reverseOrder()))
            .limit(searchQuery.howMany)
            .collect(Collectors.toList());

    try {
      return new SearchResult<>(
          resultHits,
          tookMicros,
          totalCount,
          histogram.isPresent() ? histogram.get().getBuckets() : Collections.emptyList(),
          failedNodes,
          totalNodes,
          totalSnapshots,
          snapshpotReplicas);
    } finally {
      span.finish();
    }
  }
}
