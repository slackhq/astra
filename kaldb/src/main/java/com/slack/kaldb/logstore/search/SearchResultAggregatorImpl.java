package com.slack.kaldb.logstore.search;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.opensearch.KaldbBigArrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.opensearch.search.aggregations.InternalAggregation;

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
  public SearchResult<T> aggregate(List<SearchResult<T>> searchResults) {
    long tookMicros = 0;
    int failedNodes = 0;
    int totalNodes = 0;
    int totalSnapshots = 0;
    int snapshpotReplicas = 0;
    int totalCount = 0;
    InternalAggregation internalAggregation = null;

    for (SearchResult<T> searchResult : searchResults) {
      tookMicros = Math.max(tookMicros, searchResult.tookMicros);
      failedNodes += searchResult.failedNodes;
      totalNodes += searchResult.totalNodes;
      totalSnapshots += searchResult.totalSnapshots;
      snapshpotReplicas += searchResult.snapshotsWithReplicas;
      totalCount += searchResult.totalCount;

      if (internalAggregation == null) {
        internalAggregation = searchResult.internalAggregation;
      } else {
        if (searchResult.internalAggregation != null) {
          internalAggregation =
              internalAggregation.reduce(
                  List.of(internalAggregation, searchResult.internalAggregation),
                  InternalAggregation.ReduceContext.forPartialReduction(
                      KaldbBigArrays.getInstance(), null, null));
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
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshpotReplicas,
        internalAggregation);
  }
}
