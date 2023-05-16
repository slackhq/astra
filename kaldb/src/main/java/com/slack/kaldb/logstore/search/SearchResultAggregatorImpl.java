package com.slack.kaldb.logstore.search;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.opensearch.KaldbBigArrays;
import com.slack.kaldb.logstore.opensearch.OpenSearchAdapter;
import com.slack.kaldb.logstore.opensearch.ScriptServiceProvider;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;

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
  public SearchResult<T> aggregate(List<SearchResult<T>> searchResults, boolean finalAggregation) {
    long tookMicros = 0;
    int failedNodes = 0;
    int totalNodes = 0;
    int totalSnapshots = 0;
    int snapshpotReplicas = 0;
    List<InternalAggregation> internalAggregationList = new ArrayList<>();

    for (SearchResult<T> searchResult : searchResults) {
      tookMicros = Math.max(tookMicros, searchResult.tookMicros);
      failedNodes += searchResult.failedNodes;
      totalNodes += searchResult.totalNodes;
      totalSnapshots += searchResult.totalSnapshots;
      snapshpotReplicas += searchResult.snapshotsWithReplicas;
      if (searchResult.internalAggregation != null) {
        internalAggregationList.add(searchResult.internalAggregation);
      }
    }

    InternalAggregation internalAggregation = null;
    if (internalAggregationList.size() > 0) {
      InternalAggregation.ReduceContext reduceContext;
      PipelineAggregator.PipelineTree pipelineTree = null;
      // The last aggregation should be indicated using the final aggregation boolean. This performs
      // some final pass "destructive" actions, such as applying min doc count or extended bounds.
      if (finalAggregation) {
        pipelineTree =
            OpenSearchAdapter.getAggregationBuilder(searchQuery.aggBuilder).buildPipelineTree();
        reduceContext =
            InternalAggregation.ReduceContext.forFinalReduction(
                KaldbBigArrays.getInstance(),
                ScriptServiceProvider.getInstance(),
                (s) -> {},
                pipelineTree);
      } else {
        reduceContext =
            InternalAggregation.ReduceContext.forPartialReduction(
                KaldbBigArrays.getInstance(),
                ScriptServiceProvider.getInstance(),
                () -> PipelineAggregator.PipelineTree.EMPTY);
      }
      // Using the first element on the list as the basis for the reduce method is per OpenSearch
      // recommendations: "For best efficiency, when implementing, try reusing an existing instance
      // (typically the first in the given list) to save on redundant object construction."
      internalAggregation =
          internalAggregationList.get(0).reduce(internalAggregationList, reduceContext);

      if (finalAggregation) {
        // materialize any parent pipelines
        internalAggregation =
            internalAggregation.reducePipelines(internalAggregation, reduceContext, pipelineTree);
        // materialize any sibling pipelines at top level
        for (PipelineAggregator pipelineAggregator : pipelineTree.aggregators()) {
          internalAggregation = pipelineAggregator.reduce(internalAggregation, reduceContext);
        }
      }
    }

    // TODO: Instead of sorting all hits using a bounded priority queue of size k is more efficient.
    List<T> resultHits =
        searchResults
            .stream()
            .flatMap(r -> r.hits.stream())
            .sorted(
                Comparator.comparing(
                    (T m) -> m.getTimestamp().toEpochMilli(), Comparator.reverseOrder()))
            .limit(searchQuery.howMany)
            .toList();

    return new SearchResult<>(
        resultHits,
        tookMicros,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshpotReplicas,
        internalAggregation);
  }
}
