package com.slack.kaldb.logstore.search;

import com.slack.kaldb.histogram.FixedIntervalHistogramImpl;
import com.slack.kaldb.histogram.Histogram;
import com.slack.kaldb.logstore.LogMessage;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
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
  public SearchResult<T> aggregate(
      List<SearchResult<T>> searchResults,
      int totalSnapshots,
      int skippedSnapshots,
      int requestedSnapshots) {
    long tookMicros = 0;
    int successfulSnapshots = 0;
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
      successfulSnapshots += searchResult.successfulSnapshots;
      totalCount += searchResult.totalCount;
      histogram.ifPresent(value -> value.mergeHistogram(searchResult.buckets));
    }

    // We report the failed snapshots when we get a successful response back from a node - however
    // we still need to account for nodes we tried to query and just completely timed out
    int failedSnapshots = requestedSnapshots - successfulSnapshots;

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
        totalSnapshots,
        skippedSnapshots,
        failedSnapshots,
        successfulSnapshots);
  }
}
