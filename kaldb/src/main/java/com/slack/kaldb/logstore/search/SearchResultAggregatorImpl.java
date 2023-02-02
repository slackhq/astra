package com.slack.kaldb.logstore.search;

import com.slack.kaldb.histogram.FixedIntervalHistogramImpl;
import com.slack.kaldb.histogram.Histogram;
import com.slack.kaldb.logstore.LogMessage;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.InternalAggregation;

/**
 * This class will merge multiple search results into a single search result. Takes all the hits
 * from all the search results and returns the topK most recent results. The histogram will be
 * merged using the histogram merge function.
 */
public class SearchResultAggregatorImpl<T extends LogMessage> implements SearchResultAggregator<T> {

  final BigArrays bigArrays =
      new BigArrays(
          PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService(), "none");

  private final SearchQuery searchQuery;

  public SearchResultAggregatorImpl(SearchQuery searchQuery) {
    this.searchQuery = searchQuery;
  }

  @Override
  public SearchResult<T> aggregate(List<SearchResult<T>> searchResults) {
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
    InternalAggregation internalAggregation = null;

    for (SearchResult<T> searchResult : searchResults) {
      tookMicros = Math.max(tookMicros, searchResult.tookMicros);
      failedNodes += searchResult.failedNodes;
      totalNodes += searchResult.totalNodes;
      totalSnapshots += searchResult.totalSnapshots;
      snapshpotReplicas += searchResult.snapshotsWithReplicas;
      totalCount += searchResult.totalCount;
      histogram.ifPresent(value -> value.mergeHistogram(searchResult.buckets));

      if (internalAggregation == null) {
        internalAggregation = searchResult.internalAggregation;
      } else {
        if (searchResult.internalAggregation != null) {
          internalAggregation = internalAggregation.reduce(List.of(internalAggregation, searchResult.internalAggregation), InternalAggregation.ReduceContext.forPartialReduction(bigArrays, null, null));
        }
      }
    }

    // TODO: Instead of sorting all hits using a bounded priority queue of size k is more efficient.
    List<T> resultHits =
        searchResults
            .stream()
            .flatMap(r -> r.hits.stream())
            .sorted(Comparator.comparing((T m) -> m.timeSinceEpochMilli, Comparator.reverseOrder()))
            .limit(searchQuery.howMany)
            .collect(Collectors.toList());

    return new SearchResult<>(
        resultHits,
        tookMicros,
        totalCount,
        histogram.isPresent() ? histogram.get().getBuckets() : Collections.emptyList(),
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshpotReplicas,
        internalAggregation);
  }
}
