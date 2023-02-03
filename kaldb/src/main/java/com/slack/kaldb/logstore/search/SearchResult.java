package com.slack.kaldb.logstore.search;

import com.google.common.base.Objects;
import com.slack.kaldb.logstore.LogMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.opensearch.search.aggregations.InternalAggregation;

public class SearchResult<T> {

  private static final SearchResult EMPTY =
      new SearchResult<>(Collections.emptyList(), 0, 0, 1, 1, 0, 0, null);

  public final long totalCount;

  // TODO: Make hits an iterator.
  // An iterator helps with the early termination of a search and may be efficient in some cases.
  public final List<T> hits;
  public final long tookMicros;

  public final int failedNodes;
  public final int totalNodes;
  public final int totalSnapshots;
  public final int snapshotsWithReplicas;

  public final InternalAggregation internalAggregation;

  public SearchResult() {
    this.hits = new ArrayList<>();
    this.tookMicros = 0;
    this.totalCount = 0;
    this.failedNodes = 0;
    this.totalNodes = 0;
    this.totalSnapshots = 0;
    this.snapshotsWithReplicas = 0;
    this.internalAggregation = null;
  }

  // TODO: Move stats into a separate struct.
  public SearchResult(
      List<T> hits,
      long tookMicros,
      long totalCount,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas,
      InternalAggregation internalAggregation) {
    this.hits = hits;
    this.tookMicros = tookMicros;
    this.totalCount = totalCount;
    this.failedNodes = failedNodes;
    this.totalNodes = totalNodes;
    this.totalSnapshots = totalSnapshots;
    this.snapshotsWithReplicas = snapshotsWithReplicas;
    this.internalAggregation = internalAggregation;
  }

  @Deprecated
  public SearchResult(
      List<T> hits,
      long tookMicros,
      long totalCount,
      List<Object> buckets,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas,
      InternalAggregation internalAggregation) {
    this.hits = hits;
    this.tookMicros = tookMicros;
    this.totalCount = totalCount;
    this.failedNodes = failedNodes;
    this.totalNodes = totalNodes;
    this.totalSnapshots = totalSnapshots;
    this.snapshotsWithReplicas = snapshotsWithReplicas;
    this.internalAggregation = internalAggregation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SearchResult<?> that = (SearchResult<?>) o;
    return totalCount == that.totalCount
        && tookMicros == that.tookMicros
        && failedNodes == that.failedNodes
        && totalNodes == that.totalNodes
        && totalSnapshots == that.totalSnapshots
        && snapshotsWithReplicas == that.snapshotsWithReplicas
        && Objects.equal(hits, that.hits)
        && Objects.equal(internalAggregation, that.internalAggregation);
  }

  @Override
  public String toString() {
    return "SearchResult{"
        + "totalCount="
        + totalCount
        + ", hits="
        + hits
        + ", tookMicros="
        + tookMicros
        + ", failedNodes="
        + failedNodes
        + ", totalNodes="
        + totalNodes
        + ", totalSnapshots="
        + totalSnapshots
        + ", snapshotsWithReplicas="
        + snapshotsWithReplicas
        + ", internalAggregation="
        + internalAggregation
        + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        totalCount,
        hits,
        tookMicros,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshotsWithReplicas,
        internalAggregation);
  }

  public static SearchResult<LogMessage> empty() {
    return EMPTY;
  }
}
