package com.slack.kaldb.chunk;

import com.google.common.annotations.VisibleForTesting;
import com.slack.kaldb.logstore.search.LogIndexSearcher;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import java.io.Closeable;
import java.io.IOException;

/**
 * A chunk stores messages for a specific time range. It can concurrently store messages and respond
 * to queries. Optionally a chunk can be read only at which point it can only be queried.
 */
public interface Chunk<T> extends Closeable {

  /* A string that uniquely identifies this chunk. */
  String id();

  /**
   * Given a id return a list of points contained for that id.
   *
   * @param query a Metric query.
   * @return a list of points.
   */
  SearchResult<T> query(SearchQuery query);

  ChunkInfo info();

  /** Return true if the chunk contains data within that time range. */
  boolean containsDataInTimeRange(long startTs, long endTs);

  /** Close the chunk. */
  void close() throws IOException;

  @VisibleForTesting
  void setLogSearcher(LogIndexSearcher<T> logSearcher);
}
