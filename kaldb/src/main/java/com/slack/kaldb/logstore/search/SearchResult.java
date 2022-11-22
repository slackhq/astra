package com.slack.kaldb.logstore.search;

import com.google.common.base.Objects;
import com.slack.kaldb.histogram.HistogramBucket;
import com.slack.kaldb.logstore.LogMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SearchResult<T> {

  private static final SearchResult EMPTY =
      new SearchResult<>(Collections.emptyList(), 0, 0, Collections.emptyList(), 0, 0, 0);

  public final long totalCount;

  // TODO: Make hits an iterator.
  // An iterator helps with the early termination of a search and may be efficient in some cases.
  public final List<T> hits;
  public final long tookMicros;

  // TODO: Make this list immutable?
  // TODO: Instead of histogram bucket, return tuple.
  public final List<HistogramBucket> buckets;

  // the total of possible snapshots present - requested or not
  public final int totalSnapshots;

  // the number of snapshots that failed in a request
  public final int failedSnapshots;

  // the number of snapshots that succeeded a request
  public final int successfulSnapshots;

  public SearchResult() {
    this.hits = new ArrayList<>();
    this.tookMicros = 0;
    this.totalCount = 0;
    this.buckets = new ArrayList<>();
    this.totalSnapshots = 0;
    this.failedSnapshots = 0;
    this.successfulSnapshots = 0;
  }

  // TODO: Move stats into a separate struct.
  public SearchResult(
      List<T> hits,
      long tookMicros,
      long totalCount,
      List<HistogramBucket> buckets,
      int totalSnapshots,
      int failedSnapshots,
      int successfulSnapshots) {
    this.hits = hits;
    this.tookMicros = tookMicros;
    this.totalCount = totalCount;
    this.buckets = buckets;
    this.totalSnapshots = totalSnapshots;
    this.failedSnapshots = failedSnapshots;
    this.successfulSnapshots = successfulSnapshots;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SearchResult<?> that = (SearchResult<?>) o;
    return totalCount == that.totalCount
        && tookMicros == that.tookMicros
        && totalSnapshots == that.totalSnapshots
        && failedSnapshots == that.failedSnapshots
        && successfulSnapshots == that.successfulSnapshots
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
        totalSnapshots,
        failedSnapshots,
        successfulSnapshots);
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
        + ", buckets="
        + buckets
        + ", totalSnapshots="
        + totalSnapshots
        + ", failedSnapshots="
        + failedSnapshots
        + ", successfulSnapshots="
        + successfulSnapshots
        + '}';
  }
}
