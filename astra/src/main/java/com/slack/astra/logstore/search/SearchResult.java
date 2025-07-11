package com.slack.astra.logstore.search;

import com.slack.astra.logstore.LogMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.opensearch.search.aggregations.InternalAggregation;

public class SearchResult<T> implements Cloneable {

  private static final SearchResult EMPTY =
      new SearchResult<>(Collections.emptyList(), 0, 0, 1, 0, 0, null);

  // Astra problem (instead of a user-caused issue)
  private static final SearchResult ASTRA_ERROR =
      new SearchResult<>(Collections.emptyList(), 0, 1, 1, 0, 0, null);

  private static final SearchResult USER_ERROR =
      new SearchResult<>(Collections.emptyList(), 0, 0, 0, 1, 0, null);

  // TODO: Make hits an iterator.
  // An iterator helps with the early termination of a search and may be efficient in some cases.
  public final List<T> hits;
  public final long tookMicros;

  public final int failedNodes;
  public final int totalNodes;
  public final int totalSnapshots;
  public final int snapshotsWithReplicas;
  public List<String> hardFailedChunkIds;
  public List<String> softFailedChunkIds;

  public final InternalAggregation internalAggregation;

  public SearchResult() {
    this.hits = new ArrayList<>();
    this.tookMicros = 0;
    this.failedNodes = 0;
    this.totalNodes = 0;
    this.totalSnapshots = 0;
    this.snapshotsWithReplicas = 0;
    this.internalAggregation = null;
    this.hardFailedChunkIds = new ArrayList<>();
    this.softFailedChunkIds = new ArrayList<>();
  }

  // TODO: Move stats into a separate struct.
  public SearchResult(
      List<T> hits,
      long tookMicros,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas,
      InternalAggregation internalAggregation) {
    this(
        hits,
        tookMicros,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshotsWithReplicas,
        internalAggregation,
        new ArrayList<>(),
        new ArrayList<>());
  }

  public SearchResult(
      List<T> hits,
      long tookMicros,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas,
      InternalAggregation internalAggregation,
      List<String> hardFailedChunkIds,
      List<String> softFailedChunkIds) {
    this.hits = hits;
    this.tookMicros = tookMicros;
    this.failedNodes = failedNodes;
    this.totalNodes = totalNodes;
    this.totalSnapshots = totalSnapshots;
    this.snapshotsWithReplicas = snapshotsWithReplicas;
    this.internalAggregation = internalAggregation;
    this.hardFailedChunkIds = hardFailedChunkIds;
    this.softFailedChunkIds = softFailedChunkIds;
  }

  @Override
  public String toString() {
    return "SearchResult{"
        + "hits="
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
        + ", hardFailedChunkIds="
        + hardFailedChunkIds
        + ", softFailedChunkIds="
        + softFailedChunkIds
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SearchResult<?> that = (SearchResult<?>) o;

    if (tookMicros != that.tookMicros) return false;
    if (failedNodes != that.failedNodes) return false;
    if (totalNodes != that.totalNodes) return false;
    if (totalSnapshots != that.totalSnapshots) return false;
    if (snapshotsWithReplicas != that.snapshotsWithReplicas) return false;
    if (!hits.equals(that.hits)) return false;
    if (!hardFailedChunkIds.equals(that.hardFailedChunkIds)) return false;
    if (!softFailedChunkIds.equals(that.softFailedChunkIds)) return false;

    // todo - this is pending a PR to OpenSearch to address
    // https://github.com/opensearch-project/OpenSearch/pull/6357
    // this is because DocValueFormat.DateTime in OpenSearch does not implement a proper equals
    // method
    // As such the DocValueFormat.parser are never equal to each other
    if (internalAggregation == null && that.internalAggregation == null) {
      return true;
    }

    return internalAggregation.toString().equals(that.internalAggregation.toString());
  }

  @Override
  public int hashCode() {
    int result = hits.hashCode();
    result = 31 * result + (int) (tookMicros ^ (tookMicros >>> 32));
    result = 31 * result + failedNodes;
    result = 31 * result + totalNodes;
    result = 31 * result + totalSnapshots;
    result = 31 * result + snapshotsWithReplicas;
    result = 31 * result + internalAggregation.hashCode();
    result = 31 * result + hardFailedChunkIds.hashCode();
    result = 31 * result + softFailedChunkIds.hashCode();
    return result;
  }

  public void setHardFailedChunkIds(List<String> hardFailedChunkIds) {
    this.hardFailedChunkIds = hardFailedChunkIds;
  }

  public void setSoftFailedChunkIds(List<String> softFailedChunkIds) {
    this.softFailedChunkIds = softFailedChunkIds;
  }

  public static SearchResult<LogMessage> empty() {
    return EMPTY;
  }

  public static SearchResult<LogMessage> error() {
    return ASTRA_ERROR;
  }

  public static SearchResult<LogMessage> soft_error() {
    return USER_ERROR;
  }

  @Override
  public SearchResult<T> clone() {
    try {
      SearchResult<T> clone = (SearchResult<T>) super.clone();
      clone.hardFailedChunkIds = new ArrayList<>(hardFailedChunkIds);
      clone.softFailedChunkIds = new ArrayList<>(softFailedChunkIds);
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new AssertionError();
    }
  }
}
