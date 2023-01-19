package com.slack.kaldb.logstore.search;

import com.google.common.base.Objects;
import com.slack.kaldb.logstore.LogMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SearchResult<T> {

  private static final SearchResult EMPTY =
      new SearchResult<>(Collections.emptyList(), 0, 0, Collections.emptyList(), 1, 1, 0, 0);

  public final long totalCount;

  // TODO: Make hits an iterator.
  // An iterator helps with the early termination of a search and may be efficient in some cases.
  public final List<T> hits;
  public final long tookMicros;

  public final List<ResponseAggregation> aggregations;

  public final int failedNodes;
  public final int totalNodes;
  public final int totalSnapshots;
  public final int snapshotsWithReplicas;

  public SearchResult() {
    this.hits = new ArrayList<>();
    this.tookMicros = 0;
    this.totalCount = 0;
    this.aggregations = new ArrayList<>();
    this.failedNodes = 0;
    this.totalNodes = 0;
    this.totalSnapshots = 0;
    this.snapshotsWithReplicas = 0;
  }

  // TODO: Move stats into a separate struct.
  public SearchResult(
      List<T> hits,
      long tookMicros,
      long totalCount,
      List<ResponseAggregation> aggregations,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas) {
    this.hits = hits;
    this.tookMicros = tookMicros;
    this.totalCount = totalCount;
    this.aggregations = aggregations;
    this.failedNodes = failedNodes;
    this.totalNodes = totalNodes;
    this.totalSnapshots = totalSnapshots;
    this.snapshotsWithReplicas = snapshotsWithReplicas;
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
        && Objects.equal(aggregations, that.aggregations);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        totalCount,
        hits,
        tookMicros,
        aggregations,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshotsWithReplicas);
  }

  public static SearchResult<LogMessage> empty() {
    return EMPTY;
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
        + ", aggregations="
        + aggregations
        + ", failedNodes="
        + failedNodes
        + ", totalNodes="
        + totalNodes
        + ", totalSnapshots="
        + totalSnapshots
        + ", snapshotsWithReplicas="
        + snapshotsWithReplicas
        + '}';
  }
}
