package com.slack.astra.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.opensearch.AstraBigArrays;
import com.slack.astra.logstore.opensearch.ScriptServiceProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.opensearch.search.aggregations.AggregationBuilder;
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

  /**
   * Builds a comparator for sorting LogMessages based on the provided sort specifications. Defaults
   * to timestamp descending if no sort fields are specified.
   */
  private Comparator<T> buildComparator(List<SearchQuery.SortSpec> sortFields) {
    // Default to timestamp descending if no sort fields provided
    if (sortFields == null || sortFields.isEmpty()) {
      return Comparator.comparing(
          (T m) -> m.getTimestamp().toEpochMilli(), Comparator.reverseOrder());
    }

    // Build chained comparator from sort fields
    Comparator<T> comparator = null;
    for (SearchQuery.SortSpec spec : sortFields) {
      Comparator<T> fieldComparator = buildFieldComparator(spec);
      comparator =
          (comparator == null) ? fieldComparator : comparator.thenComparing(fieldComparator);
    }
    return comparator;
  }

  /**
   * Builds a comparator for a single sort field. Extracts the field value from the LogMessage
   * source and compares using natural ordering. Handles nulls by placing them at the end.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Comparator<T> buildFieldComparator(SearchQuery.SortSpec spec) {
    Comparator<T> comparator =
        (T m1, T m2) -> {
          Object value1;
          Object value2;

          // Handle system fields specially
          if (LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName.equals(spec.fieldName)) {
            // For timestamp field, use getTimestamp() instead of source map
            value1 = m1.getTimestamp().toEpochMilli();
            value2 = m2.getTimestamp().toEpochMilli();
          } else if (LogMessage.SystemField.ID.fieldName.equals(spec.fieldName)) {
            // For ID field, use getId() instead of source map
            value1 = m1.getId();
            value2 = m2.getId();
          } else {
            // For regular fields, get from source map
            value1 = m1.getSource().get(spec.fieldName);
            value2 = m2.getSource().get(spec.fieldName);
          }

          // Handle nulls - always put them last
          if (value1 == null && value2 == null) return 0;
          if (value1 == null) return 1; // null goes after non-null
          if (value2 == null) return -1; // non-null goes before null

          // Compare using natural ordering if comparable
          if (value1 instanceof Comparable && value2 instanceof Comparable) {
            return ((Comparable) value1).compareTo(value2);
          }

          // Fallback to string comparison
          return String.valueOf(value1).compareTo(String.valueOf(value2));
        };

    return spec.isDescending ? comparator.reversed() : comparator;
  }

  @Override
  public SearchResult<T> aggregate(List<SearchResult<T>> searchResults, boolean finalAggregation) {
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("SearchResultAggregatorImpl.aggregate");
    long tookMicros = 0;
    int failedNodes = 0;
    int totalNodes = 0;
    int totalSnapshots = 0;
    int snapshpotReplicas = 0;
    List<InternalAggregation> internalAggregationList = new ArrayList<>();
    List<String> hardFailedChunkIds = new ArrayList<>();
    List<String> softFailedChunkIds = new ArrayList<>();

    for (SearchResult<T> searchResult : searchResults) {
      tookMicros = Math.max(tookMicros, searchResult.tookMicros);
      failedNodes += searchResult.failedNodes;
      totalNodes += searchResult.totalNodes;
      totalSnapshots += searchResult.totalSnapshots;
      snapshpotReplicas += searchResult.snapshotsWithReplicas;
      if (searchResult.internalAggregation != null) {
        internalAggregationList.add(searchResult.internalAggregation);
      }

      if (searchResult.hardFailedChunkIds != null) {
        hardFailedChunkIds.addAll(searchResult.hardFailedChunkIds);
      }

      if (searchResult.softFailedChunkIds != null) {
        softFailedChunkIds.addAll(searchResult.softFailedChunkIds);
      }
    }

    InternalAggregation internalAggregation = null;
    if (internalAggregationList.size() > 0) {
      InternalAggregation.ReduceContext reduceContext;
      PipelineAggregator.PipelineTree pipelineTree = null;
      // The last aggregation should be indicated using the final aggregation boolean. This performs
      // some final pass "destructive" actions, such as applying min doc count or extended bounds.
      if (finalAggregation) {
        if (searchQuery.aggregatorFactoriesBuilder != null) {
          Collection<AggregationBuilder> aggregationBuilders =
              searchQuery.aggregatorFactoriesBuilder.getAggregatorFactories();
          pipelineTree = aggregationBuilders.iterator().next().buildPipelineTree();
        }

        reduceContext =
            InternalAggregation.ReduceContext.forFinalReduction(
                AstraBigArrays.getInstance(),
                ScriptServiceProvider.getInstance(),
                (s) -> {},
                pipelineTree);
      } else {
        reduceContext =
            InternalAggregation.ReduceContext.forPartialReduction(
                AstraBigArrays.getInstance(),
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
    // Build comparator from sort fields, defaulting to timestamp descending
    Comparator<T> comparator = buildComparator(searchQuery.sortFields);

    List<T> resultHits =
        searchResults.stream()
            .flatMap(r -> r.hits.stream())
            .sorted(comparator)
            .limit(searchQuery.howMany)
            .collect(Collectors.toList());

    span.tag("resultHits", String.valueOf(resultHits.size()));
    span.tag("finalAggregation", String.valueOf(finalAggregation));
    span.finish();

    return new SearchResult<>(
        resultHits,
        tookMicros,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshpotReplicas,
        internalAggregation,
        hardFailedChunkIds,
        softFailedChunkIds);
  }
}
