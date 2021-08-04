package com.slack.kaldb.logstore.search;

import com.google.common.base.Objects;
import com.slack.kaldb.histogram.HistogramBucket;
import java.util.ArrayList;
import java.util.List;

public class SearchResult<T> {

  private static final SearchResult EMPTY = new SearchResult<>(null, 0, 0, null, 1, 1, 0, 0);

  public final long totalCount;

  // TODO: Make hits an iterator.
  // An iterator helps with the early termination of a search and may be efficient in some cases.
  public final List<T> hits;
  public final long tookMicros;

  // TODO: Make this list immutable?
  // TODO: Instead of histogram bucket, return tuple.
  public final List<HistogramBucket> buckets;

  public final int failedNodes;
  public final int totalNodes;
  public final int totalSnapshots;
  public final int snapshotsWithReplicas;

  public SearchResult() {
    this.hits = new ArrayList<>();
    this.tookMicros = 0;
    this.totalCount = 0;
    this.buckets = new ArrayList<>();
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
      List<HistogramBucket> buckets,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas) {
    this.hits = hits;
    this.tookMicros = tookMicros;
    this.totalCount = totalCount;
    this.buckets = buckets;
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
        && Objects.equal(buckets, that.buckets);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        totalCount,
        hits,
        tookMicros,
        buckets,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshotsWithReplicas);
  }

  public static SearchResult empty() {
    return EMPTY;
  }
}
