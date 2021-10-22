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
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
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
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final SearchMetadataStore searchMetadataStore;
  private final SearchContext searchContext;
  private final SearchMetadata liveSearchMetadata;
  private final SnapshotMetadata liveSnapshotMetadata;
  private LogIndexSearcher<T> logSearcher;
  private final Counter fileUploadAttempts;
  private final Counter fileUploadFailures;
  private final MeterRegistry meterRegistry;
  // TODO: Export file size uploaded as a metric.
  // TODO: Add chunk info as tags?.

  // TODO: Move this flag into LogStore?.
  private boolean readOnly;

  public ReadWriteChunkImpl(
      LogStore<T> logStore,
      String chunkDataPrefix,
      MeterRegistry meterRegistry,
      SearchMetadataStore searchMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      SearchContext searchContext) {
    this.logStore = logStore;
    this.logSearcher =
        (LogIndexSearcher<T>) new LogIndexSearcherImpl(logStore.getSearcherManager());

    // Create chunk metadata
    Instant chunkCreationTime = Instant.now();
    chunkInfo =
        new ChunkInfo(
            chunkDataPrefix + "_" + chunkCreationTime.toEpochMilli(),
            chunkCreationTime.toEpochMilli());
    this.readOnly = false;
    this.fileUploadAttempts = meterRegistry.counter(INDEX_FILES_UPLOAD);
    this.fileUploadFailures = meterRegistry.counter(INDEX_FILES_UPLOAD_FAILED);
    this.meterRegistry = meterRegistry;
    liveSnapshotMetadata = toSnapshotMetadata(chunkInfo);
    liveSearchMetadata = toSearchMetadata(SearchMetadata.LIVE_SNAPSHOT_NAME, searchContext);
    this.searchMetadataStore = searchMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.searchContext = searchContext;
    LOG.info("Created a new index {} and chunk {}", logStore, chunkInfo);
  }

  public void register() {
    snapshotMetadataStore.createSync(liveSnapshotMetadata);
    searchMetadataStore.createSync(liveSearchMetadata);
  }

  private SearchMetadata toSearchMetadata(String snapshotName, SearchContext searchContext) {
    return new SearchMetadata(searchContext.hostname, snapshotName, searchContext.toUrl());
  }

  private SnapshotMetadata toSnapshotMetadata(ChunkInfo chunkInfo) {
    // TODO: Set accurate start and end offset and kafka partition id in chunk info.
    // TODO: Ensure end time is INF for now.
    // TODO: Move to chunkInfo class?
    // TODO: Does chunkInfo have all the fields we need?
    return new SnapshotMetadata(
        chunkInfo.chunkId,
        SearchMetadata.LIVE_SNAPSHOT_NAME,
        chunkInfo.chunkId,
        chunkInfo.getDataStartTimeEpochMs(),
        chunkInfo.getDataEndTimeEpochMs(),
        0,
        "");
  }

  /**
   * Index the message in the logstore and update the chunk data time range.
   *
   * @param message a LogMessage object.
   */
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
      throw new IllegalStateException(String.format("Chunk %s is read only", chunkInfo));
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
  public void close() throws IOException {
    // TODO: Remove the search node.
    logSearcher.close();
    logStore.close();
    LOG.info("Closed chunk {}", chunkInfo);

    try {
      logStore.cleanup();
      LOG.info("Cleaned up chunk {}", chunkInfo);
    } catch (Exception e) {
      // this will allow the service to still close successfully when failing to cleanup the file
      LOG.error("Failed to cleanup logstore for chunk {}", chunkInfo, e);
    }
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  public void commit() {
    logStore.commit();
    logStore.refresh();
  }

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

  public void postSnapshot() {
    // Register a non-live snapshot node and a search node. New nodes should be registered before
    // removing old nodes so there is no availability gap in the data.
    SnapshotMetadata nonLiveSnapshotMetadata = toSnapshotMetadata(chunkInfo);
    snapshotMetadataStore.createSync(nonLiveSnapshotMetadata);

    SearchMetadata nonLiveSearchMetadata = toSearchMetadata(chunkInfo.chunkId, searchContext);
    searchMetadataStore.createSync(nonLiveSearchMetadata);

    // TODO: Delete right away?
    // TODO: Set the local vars to null after removal?
    // TODO: Instead of adding new search node, should we update existing node?
    searchMetadataStore.deleteSync(liveSearchMetadata);
    snapshotMetadataStore.deleteSync(liveSnapshotMetadata);
    LOG.info("Post snapshot operation completed for RW chunk {}", chunkInfo);
  }

  @VisibleForTesting
  public void setLogSearcher(LogIndexSearcher<T> logSearcher) {
    this.logSearcher = logSearcher;
  }

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
