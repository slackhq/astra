package com.slack.kaldb.chunk;

import com.google.common.annotations.VisibleForTesting;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
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

  /**
   * add a message to a store.
   *
   * @param message a LogMessage object.
   */
  void addMessage(T message);

  /** A chunk contains some metadata like the list of chunks it can contain. */
  ChunkInfo info();

  /**
   * A chunk will be available for writes initially. But once we no longer need to write any data to
   * it can be turned into a read only chunk.
   */
  boolean isReadOnly();

  /** Return true if the chunk contains data within that time range. */
  boolean containsDataInTimeRange(long startTs, long endTs);

  /* Commit the data to disk */
  void commit();

  // Return a map of stats about the index.
  // NOTE: With metrics baked in this may be redundant or return a metrics object?
  // Map<String, Object> getStats();

  /** Close the chunk. */
  void close() throws IOException;

  /** Enable/disable read only mode for the store. */
  void setReadOnly(boolean readOnly);

  /** House keeping before the snapshot */
  void preSnapshot();

  boolean snapshotToS3(String bucket, String prefix, S3BlobFs s3BlobFs);

  /** House keeping after the snapshot */
  void postSnapshot();

  /** Cleanup the chunk */
  void cleanup();

  @VisibleForTesting
  public LogIndexSearcher<T> getLogSearcher();

  @VisibleForTesting
  public void setLogSearcher(LogIndexSearcher<T> logSearcher);
}
