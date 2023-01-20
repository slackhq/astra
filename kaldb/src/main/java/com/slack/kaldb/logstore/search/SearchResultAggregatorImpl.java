package com.slack.kaldb.logstore.search;

import com.slack.kaldb.logstore.LogMessage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
  public SearchResult<T> aggregate(List<SearchResult<T>> searchResults) {
    long tookMicros = 0;
    int failedNodes = 0;
    int totalNodes = 0;
    int totalSnapshots = 0;
    int snapshpotReplicas = 0;
    int totalCount = 0;
    List<ResponseAggregation> responseAggregationList = new ArrayList<>();

    for (SearchResult<T> searchResult : searchResults) {
      tookMicros = Math.max(tookMicros, searchResult.tookMicros);
      failedNodes += searchResult.failedNodes;
      totalNodes += searchResult.totalNodes;
      totalSnapshots += searchResult.totalSnapshots;
      snapshpotReplicas += searchResult.snapshotsWithReplicas;
      totalCount += searchResult.totalCount;

      if (responseAggregationList.isEmpty()) {
        responseAggregationList = searchResult.aggregations;
      } else {
        responseAggregationList =
            mergeAggregations(responseAggregationList, searchResult.aggregations);
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
        responseAggregationList,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshpotReplicas);
  }

  // todo - This is very ugly, but we're entirely redoing the aggregators so this will be going away
  // very soon
  private List<ResponseAggregation> mergeAggregations(
      List<ResponseAggregation> a, List<ResponseAggregation> b) {
    if (a.isEmpty()) {
      return b;
    }

    if (b.isEmpty()) {
      return a;
    }

    if (a.size() != 1 || b.size() != 1) {
      throw new IllegalArgumentException();
    }

    if (!a.get(0).getName().equals(b.get(0).getName())) {
      throw new IllegalArgumentException();
    }

    if (a.get(0).getResponseBuckets().size() == 0) {
      return a;
    }

    if (b.get(0).getResponseBuckets().size() == 0) {
      return b;
    }

    if ((a.get(0).getResponseBuckets().size() != b.get(0).getResponseBuckets().size())) {
      throw new IllegalArgumentException();
    }

    List<ResponseBucket> mergedResponseBuckets = new ArrayList<>();

    for (int i = 0; i < a.get(0).getResponseBuckets().size(); i++) {
      ResponseBucket aBucket = a.get(0).getResponseBuckets().get(i);
      ResponseBucket bBucket = b.get(0).getResponseBuckets().get(i);

      if (!aBucket.getKey().equals(bBucket.getKey())) {
        throw new IllegalArgumentException();
      }

      if (!aBucket.getValues().equals(bBucket.getValues())) {
        throw new IllegalArgumentException();
      }

      mergedResponseBuckets.add(
          new ResponseBucket(
              aBucket.getKey(),
              aBucket.getDocCount() + bBucket.getDocCount(),
              aBucket.getValues()));
    }

    return List.of(
        new ResponseAggregation(
            a.get(0).getName(),
            Math.max(a.get(0).getDocCountErrorUpperBound(), b.get(0).getDocCountErrorUpperBound()),
            a.get(0).getSumOtherDocCount() + b.get(0).getSumOtherDocCount(),
            mergedResponseBuckets));
  }
}
