package com.slack.kaldb.chunk;

import static com.slack.kaldb.chunk.ChunkInfo.toSnapshotMetadata;
import static com.slack.kaldb.logstore.BlobFsUtils.copyToS3;
import static com.slack.kaldb.logstore.BlobFsUtils.createURI;

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
 * <p>Chunk maintains its metadata in the chunkInfo object. The chunkInfo tracks all the info needed
 * for constructing a snapshot.
 *
 * <p>A ReadWriteChunk goes through the following life cycle.
 *
 * <p>When a chunk is created it is open for both reads and writes. Since a ReadWriteChunk is
 * ingesting live data, a cluster manager doesn't manage it. Instead, when a chunk in created, it
 * registers a live snapshot and a live search node.
 *
 * <p>Once the chunk is full, it will be snapshotted. Once snapshotted the chunk is not open for
 * writing anymore. When a chunk is snapshotted, a non-live snapshot is created which is assigned to
 * a cache node by the cluster manager. The live snapshot is updated with the end time of the chunk
 * so it only receives the queries for the data within it's time range. As long as the chunk is up,
 * it will be searched using the live search node.
 *
 * <p>When the ReadWriteChunk is finally closed (happens when a chunk is evicted), the live snapshot
 * and the search metadata are deleted as part of the chunk de-registration process.
 */
public class ReadWriteChunkImpl<T> implements Chunk<T> {

  // TODO: Add a global UUID to identify each chunk uniquely.
  private static final Logger LOG = LoggerFactory.getLogger(ReadWriteChunkImpl.class);
  public static final String INDEX_FILES_UPLOAD = "index_files_upload";
  public static final String INDEX_FILES_UPLOAD_FAILED = "index_files_upload_failed";
  public static final String SNAPSHOT_TIMER = "snapshot.timer";
  public static final String LIVE_SNAPSHOT_PREFIX = SearchMetadata.LIVE_SNAPSHOT_PATH + "_";

  private final LogStore<T> logStore;
  private final ChunkInfo chunkInfo;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final SearchMetadataStore searchMetadataStore;
  private final SearchMetadata liveSearchMetadata;
  private SnapshotMetadata liveSnapshotMetadata;
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
      SearchContext searchContext,
      String kafkaPartitionId) {
    this.logStore = logStore;
    this.logSearcher =
        (LogIndexSearcher<T>) new LogIndexSearcherImpl(logStore.getSearcherManager());

    // Create chunk metadata
    Instant chunkCreationTime = Instant.now();
    chunkInfo =
        new ChunkInfo(
            chunkDataPrefix + "_" + chunkCreationTime.toEpochMilli(),
            chunkCreationTime.toEpochMilli(),
            kafkaPartitionId,
            SearchMetadata.LIVE_SNAPSHOT_PATH);

    readOnly = false;
    this.meterRegistry = meterRegistry;
    fileUploadAttempts = meterRegistry.counter(INDEX_FILES_UPLOAD);
    fileUploadFailures = meterRegistry.counter(INDEX_FILES_UPLOAD_FAILED);
    liveSnapshotMetadata = toSnapshotMetadata(chunkInfo, LIVE_SNAPSHOT_PREFIX);
    liveSearchMetadata = toSearchMetadata(liveSnapshotMetadata.snapshotId, searchContext);
    this.searchMetadataStore = searchMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    LOG.info("Created a new index {} and chunk {}", logStore, chunkInfo);
  }

  public void register() {
    snapshotMetadataStore.createSync(liveSnapshotMetadata);
    searchMetadataStore.createSync(liveSearchMetadata);
  }

  public void deRegister() {
    searchMetadataStore.deleteSync(liveSearchMetadata);
    snapshotMetadataStore.deleteSync(liveSnapshotMetadata);
  }

  private SearchMetadata toSearchMetadata(String snapshotName, SearchContext searchContext) {
    return new SearchMetadata(snapshotName, snapshotName, searchContext.toUrl());
  }

  /**
   * Index the message in the logstore and update the chunk data time range.
   *
   * @param message a LogMessage object.
   * @param offset
   */
  public void addMessage(T message, long offset) {
    if (!readOnly) {
      logStore.addMessage(message);
      // Update the chunk with the time range of the data in the chunk.
      // TODO: This type conversion is a temporary hack, fix it by adding timestamp field to the
      // message.
      if (message instanceof LogMessage) {
        chunkInfo.updateDataTimeRange(((LogMessage) message).timeSinceEpochMilli);
        chunkInfo.updateMaxOffset(offset);
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
    deRegister();

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
      chunkInfo.setSnapshotPath(createURI(bucket, prefix, "").toString());
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
    LOG.info("Start Post snapshot chunk {}", chunkInfo);
    // Publish a persistent snapshot for this chunk.
    SnapshotMetadata nonLiveSnapshotMetadata = toSnapshotMetadata(chunkInfo, "");
    snapshotMetadataStore.createSync(nonLiveSnapshotMetadata);

    // Update the live snapshot. Keep the same snapshotId and snapshotPath to
    // ensure it's a live snapshot.
    SnapshotMetadata updatedSnapshotMetadata =
        new SnapshotMetadata(
            liveSnapshotMetadata.snapshotId,
            liveSnapshotMetadata.snapshotPath,
            chunkInfo.getDataStartTimeEpochMs(),
            chunkInfo.getDataEndTimeEpochMs(),
            chunkInfo.getMaxOffset(),
            chunkInfo.getKafkaPartitionId());
    snapshotMetadataStore.updateSync(updatedSnapshotMetadata);
    liveSnapshotMetadata = updatedSnapshotMetadata;

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
