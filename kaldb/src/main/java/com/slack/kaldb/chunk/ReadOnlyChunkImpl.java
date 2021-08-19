package com.slack.kaldb.chunk;

import com.google.common.annotations.VisibleForTesting;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.logstore.search.LogIndexSearcher;
import com.slack.kaldb.logstore.search.LogIndexSearcherImpl;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ReadOnlyChunkImpl provides a concrete implementation for a shard to which we can support reads
 * but not writes.
 *
 * <p>This would be a wrapper around the log searcher interface without creating a heavier logStore
 * object. This implementation is also safer since we don't open the files for writing.
 *
 * <p>This class will be read only by default.
 *
 * <p>It is unclear now if the snapshot functions should be in this class or not. So, for now, we
 * leave them as no-ops.
 *
 * <p>TODO: In future, make chunkInfo read only for more safety.
 *
 * <p>TODO: Is chunk responsible for maintaining it's own metadata?
 *
 * <p>TODO: Move common operations between RO and RW chunk stores into a base class.
 */
public class ReadOnlyChunkImpl<T> implements Chunk<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyChunkImpl.class);

  private final ChunkInfo chunkInfo;
  private LogIndexSearcher<T> logSearcher;

  // TODO: Move this flag into LogStore?.
  private final boolean readOnly;

  public ReadOnlyChunkImpl(Path dataDirectory, ChunkInfo chunkInfo, MeterRegistry metricsRegistry)
      throws IOException {
    this.logSearcher =
        (LogIndexSearcher<T>)
            new LogIndexSearcherImpl(LogIndexSearcherImpl.searcherManagerFromPath(dataDirectory));
    this.chunkInfo = chunkInfo;
    this.readOnly = true;
    LOG.info("Created a new read only chunk {}", chunkInfo);
  }

  @Override
  public void addMessage(T message) {
    throw new ReadOnlyChunkInsertionException(
        String.format("Chunk %s is a read only chunk", chunkInfo));
  }

  @Override
  public ChunkInfo info() {
    return chunkInfo;
  }

  @Override
  public boolean containsDataInTimeRange(long startTs, long endTs) {
    return chunkInfo.containsDataInTimeRange(startTs, endTs);
  }

  /* Since this chunk is read-only, commit is a no-op. */
  @Override
  public void commit() {}

  @Override
  public void close() throws IOException {
    logSearcher.close();
    LOG.info("Closed chunk {}", chunkInfo);
  }

  @Override
  public void setReadOnly(boolean readOnly) {
    if (!readOnly) {
      throw new UnsupportedOperationException("ReadOnly chunk can't be set to write.");
    }
  }

  @Override
  public void preSnapshot() {
    LOG.info("Finished pre-snapshot for RO chunk {}", chunkInfo);
  }

  @Override
  public boolean snapshotToS3(String bucket, String prefix, S3BlobFs s3BlobFs) {
    LOG.info("Failed snapshot to S3 for RO chunk {}", chunkInfo);
    throw new UnsupportedOperationException("ReadOnly chunk can't be snapshotted.");
  }

  @Override
  public void postSnapshot() {
    LOG.info("Finished post-snapshot for RO chunk {}", chunkInfo);
  }

  /** Deletes the log store data from local disk. Should be called after close(). */
  @Override
  public void cleanup() {
    // TODO: Implement chunk state cleanup
    throw new UnsupportedOperationException("To be implemented.");
  }

  @Override
  @VisibleForTesting
  public LogIndexSearcher<T> getLogSearcher() {
    return logSearcher;
  }

  @Override
  @VisibleForTesting
  public void setLogSearcher(LogIndexSearcher<T> logSearcher) {
    this.logSearcher = logSearcher;
  }

  @Override
  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public String id() {
    return chunkInfo.chunkId;
  }

  @Override
  public SearchResult<T> query(SearchQuery query) {
    return logSearcher.search(
        query.indexName,
        query.queryStr,
        query.startTimeEpochMs,
        query.endTimeEpochMs,
        query.howMany,
        query.bucketCount);
  }
}
