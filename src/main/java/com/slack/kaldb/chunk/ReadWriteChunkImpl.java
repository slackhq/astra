package com.slack.kaldb.chunk;

import static com.slack.kaldb.logstore.BlobFsUtils.copyToS3;

import com.google.common.annotations.VisibleForTesting;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogStore;
import com.slack.kaldb.logstore.search.LogIndexSearcher;
import com.slack.kaldb.logstore.search.LogIndexSearcherImpl;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import org.apache.lucene.index.IndexCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ReadWriteChunkImpl provides a concrete implementation for a shard to which we can write and
 * read the messages we wrote. It provides a unified interface of a shard abstracting the details of
 * the underlying storage implementation.
 *
 * <p>TODO: Is chunk responsible for maintaining it's own metadata?
 */
public class ReadWriteChunkImpl<T> implements Chunk<T> {

  // TODO: Add a global UUID to identify each chunk uniquely.

  private static final Logger LOG = LoggerFactory.getLogger(ReadWriteChunkImpl.class);
  public static final String INDEX_FILES_UPLOAD = "index_files_upload";
  public static final String INDEX_FILES_UPLOAD_FAILED = "index_files_upload_failed";
  public static final String SNAPSHOT_TIMER = "snapshot.timer";

  private final LogStore<T> logStore;
  private final ChunkInfo chunkInfo;
  private LogIndexSearcher<T> logSearcher;
  private final Counter fileUploadAttempts;
  private final Counter fileUploadFailures;
  private final MeterRegistry meterRegistry;
  // TODO: Export file size uploaded as a metric.
  // TODO: Add chunk info as tags?.

  // TODO: Move this flag into LogStore?.
  private boolean readOnly;

  public ReadWriteChunkImpl(
      LogStore<T> logStore, String chunkDataPrefix, MeterRegistry meterRegistry) {
    this.logStore = logStore;
    this.logSearcher =
        (LogIndexSearcher<T>) new LogIndexSearcherImpl(logStore.getSearcherManager());

    // Create chunk metadata
    Instant chunkCreationTime = Instant.now();
    chunkInfo =
        new ChunkInfo(
            chunkDataPrefix + "_" + chunkCreationTime.toEpochMilli(),
            chunkCreationTime.getEpochSecond());
    this.readOnly = false;
    this.fileUploadAttempts = meterRegistry.counter(INDEX_FILES_UPLOAD);
    this.fileUploadFailures = meterRegistry.counter(INDEX_FILES_UPLOAD_FAILED);
    this.meterRegistry = meterRegistry;
    LOG.info("Created a new index {} and chunk {}", logStore, chunkInfo);
  }

  /**
   * Index the message in the logstore and update the chunk data time range.
   *
   * @param message a LogMessage object.
   */
  @Override
  public void addMessage(T message) {
    if (!readOnly) {
      logStore.addMessage(message);
      // Update the chunk with the time range of the data in the chunk.
      // TODO: This type conversion is a temporary hack, fix it by adding timestamp field to the
      // message.
      if (message instanceof LogMessage) {
        chunkInfo.updateDataTimeRange(((LogMessage) message).timeSinceEpochMilli);
      }
    } else {
      throw new ReadOnlyChunkInsertionException(String.format("Chunk %s is read only", chunkInfo));
    }
  }

  @Override
  public ChunkInfo info() {
    return chunkInfo;
  }

  @Override
  public boolean containsDataInTimeRange(long startTs, long endTs) {
    return chunkInfo.containsDataInTimeRange(startTs, endTs);
  }

  @Override
  public void close() {
    logSearcher.close();
    logStore.close();
    LOG.info("Closed chunk {}", chunkInfo);
  }

  @Override
  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  @Override
  public void commit() {
    logStore.commit();
    logStore.refresh();
  }

  @Override
  public void preSnapshot() {
    LOG.info("Started RW chunk pre-snapshot {}", chunkInfo);
    setReadOnly(true);
    commit();
    LOG.info("Finished RW chunk pre-snapshot {}", chunkInfo);
  }

  /**
   * Copy the files from log store to S3 to a given bucket, prefix.
   *
   * @return true on success, false on failure.
   */
  @Override
  public boolean snapshotToS3(String bucket, String prefix, S3BlobFs s3BlobFs) {
    LOG.info("Started RW chunk snapshot to S3 {}", chunkInfo);

    IndexCommit indexCommit = null;
    try {
      Path dirPath = logStore.getDirectory().toAbsolutePath();
      indexCommit = logStore.getIndexCommit();
      Collection<String> activeFiles = indexCommit.getFileNames();
      LOG.info("{} active files in {} in index", activeFiles.size(), dirPath);
      for (String fileName : activeFiles) {
        LOG.debug("File name is {}}", fileName);
      }
      this.fileUploadAttempts.increment(activeFiles.size());
      Timer.Sample snapshotTimer = Timer.start(meterRegistry);
      final int success = copyToS3(dirPath, activeFiles, bucket, prefix, s3BlobFs);
      snapshotTimer.stop(meterRegistry.timer(SNAPSHOT_TIMER));
      this.fileUploadFailures.increment(activeFiles.size() - success);
      LOG.info("Finished RW chunk snapshot to S3 {}.", chunkInfo);
      return true;
    } catch (Exception e) {
      LOG.error("Exception when copying RW chunk " + chunkInfo + " to S3.", e);
      return false;
    } finally {
      logStore.releaseIndexCommit(indexCommit);
    }
  }

  @Override
  public void postSnapshot() {
    LOG.info("Post snapshot operation completed for RW chunk {}", chunkInfo);
  }

  /** Deletes the log store data from local disk. Should be called after close(). */
  @Override
  public void cleanup() {
    if (logStore.isOpen()) {
      throw new IllegalStateException("Clean up can only be called on a closed logstore.");
    }
    try {
      logStore.cleanup();
      LOG.info("Cleaned up chunk {}", chunkInfo);
    } catch (IOException e) {
      String msg = String.format("Error cleaning up chunk %s", chunkInfo);
      LOG.error(msg, e);
      throw new ChunkStateException(msg);
    }
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
