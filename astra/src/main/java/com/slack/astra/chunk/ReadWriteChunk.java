package com.slack.astra.chunk;

import static com.slack.astra.chunk.ChunkInfo.toSnapshotMetadata;
import static com.slack.astra.writer.SpanFormatter.isValidTimestamp;

import com.google.common.annotations.VisibleForTesting;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.logstore.LogStore;
import com.slack.astra.logstore.LuceneIndexStoreImpl;
import com.slack.astra.logstore.search.LogIndexSearcher;
import com.slack.astra.logstore.search.LogIndexSearcherImpl;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.metadata.schema.ChunkSchema;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.lucene.index.IndexCommit;
import org.slf4j.Logger;

/**
 * An ReadWriteChunk provides a base implementation for a shard to which we can write and read the
 * messages we wrote. It provides a unified interface of a shard abstracting the details of the
 * underlying storage implementation. There will be 2 implementation for this class, one in the
 * indexer and one in recovery process. The code that's common for both of these classes will reside
 * in the base class.
 *
 * <p>A ReadWriteChunk maintains its metadata in the chunkInfo object. For example, the data in the
 * chunkInfo object can be used when publishing a snapshot from the chunk.
 *
 * <p>A ReadWriteChunk provides methods that let its users hook into various life cycle events of
 * the chunk. The hooks into the life cycle are implemented as abstract base methods so derived
 * classes can take custom action on those stages.
 *
 * <p>When a chunk is created it is open for both reads and writes. Since a ReadWriteChunk is
 * ingesting live data, a cluster manager doesn't manage it. The postCreate and preClose methods
 * provide hooks to handle the metadata registration in those cases. The postCreate method is called
 * after the chunk is created and preClose method is called just before chunk close.
 *
 * <p>Once the chunk is full, it will be snapshotted. Once snapshotted the chunk is not open for
 * writing anymore. The snapshotting process consists of 3 steps implemented by the following
 * methods: preSnapshot, snapshotToS3 and postSnapshot. Currently, only the postSnapshot is
 * implemented as an abstract method since we don't foresee any customization for the other two
 * steps.
 *
 * <p>When the ReadWriteChunk is finally closed (happens when a chunk is evicted), the preClose
 * method is called to manage any metadata.
 */
public abstract class ReadWriteChunk<T> implements Chunk<T> {
  // TODO: Add a global UUID to identify each chunk uniquely.
  public static final String INDEX_FILES_UPLOAD = "index_files_upload";
  public static final String INDEX_FILES_UPLOAD_FAILED = "index_files_upload_failed";
  public static final String SNAPSHOT_TIMER = "snapshot.timer";
  public static final String LIVE_SNAPSHOT_PREFIX = "LIVE_";
  public static final String SCHEMA_FILE_NAME = "schema.json";

  private final LogStore logStore;
  private final String kafkaPartitionId;
  private final Logger logger;
  private LogIndexSearcher<T> logSearcher;
  private final Counter fileUploadAttempts;
  private final MeterRegistry meterRegistry;
  protected final ChunkInfo chunkInfo;
  protected final SearchMetadata liveSearchMetadata;
  protected SnapshotMetadata liveSnapshotMetadata;
  protected final SnapshotMetadataStore snapshotMetadataStore;
  protected final SearchMetadataStore searchMetadataStore;
  // TODO: Export file size uploaded as a metric.
  // TODO: Add chunk info as tags?.

  // TODO: Move this flag into LogStore?.
  private boolean readOnly;

  protected ReadWriteChunk(
      LogStore logStore,
      String chunkDataPrefix,
      MeterRegistry meterRegistry,
      SearchMetadataStore searchMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      SearchContext searchContext,
      String kafkaPartitionId,
      Logger logger) {
    // TODO: Add checkArgument for the fields.
    this.logStore = logStore;
    String logStoreId = ((LuceneIndexStoreImpl) logStore).getId();
    this.logSearcher =
        (LogIndexSearcher<T>)
            new LogIndexSearcherImpl(logStore.getAstraSearcherManager(), logStore.getSchema());

    // Create chunk metadata
    Instant chunkCreationTime = Instant.now();
    this.kafkaPartitionId = kafkaPartitionId;
    chunkInfo =
        new ChunkInfo(
            chunkDataPrefix + "_" + chunkCreationTime.getEpochSecond() + "_" + logStoreId,
            chunkCreationTime.toEpochMilli(),
            kafkaPartitionId);

    readOnly = false;
    this.meterRegistry = meterRegistry;
    fileUploadAttempts = meterRegistry.counter(INDEX_FILES_UPLOAD);
    liveSnapshotMetadata = toSnapshotMetadata(chunkInfo, LIVE_SNAPSHOT_PREFIX);
    liveSearchMetadata = toSearchMetadata(liveSnapshotMetadata.snapshotId, searchContext);
    this.searchMetadataStore = searchMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.logger = logger;
    logger.info("Created a new index {} and chunk {}", logStore, chunkInfo);
  }

  /** postCreate is called by ChunkManager after a chunk is created. */
  public abstract void postCreate();

  /** preClose method is called before the chunk is closed. */
  public abstract void preClose();

  @VisibleForTesting
  public static SearchMetadata toSearchMetadata(String snapshotName, SearchContext searchContext) {
    return new SearchMetadata(
        SearchMetadata.generateSearchContextSnapshotId(snapshotName, searchContext.hostname),
        snapshotName,
        searchContext.toUrl(),
        true);
  }

  /** Index the message in the logstore and update the chunk data time range. */
  public void addMessage(Trace.Span message, String kafkaPartitionId, long offset) {
    if (!this.kafkaPartitionId.equals(kafkaPartitionId)) {
      throw new IllegalArgumentException(
          "All messages for this chunk should belong to partition: "
              + this.kafkaPartitionId
              + " not "
              + kafkaPartitionId);
    }
    if (!readOnly) {
      logStore.addMessage(message);

      Instant timestamp =
          Instant.ofEpochMilli(
              TimeUnit.MILLISECONDS.convert(message.getTimestamp(), TimeUnit.MICROSECONDS));
      if (!isValidTimestamp(timestamp)) {
        timestamp = Instant.now();
      }
      chunkInfo.updateDataTimeRange(timestamp.toEpochMilli());

      chunkInfo.updateMaxOffset(offset);
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
    preClose();

    logSearcher.close();
    logStore.close();
    logger.info("Closed chunk {}", chunkInfo);

    try {
      logStore.cleanup();
      logger.info("Cleaned up chunk {}", chunkInfo);
    } catch (Exception e) {
      // this will allow the service to still close successfully when failing to cleanup the file
      logger.error("Failed to cleanup logstore for chunk {}", chunkInfo, e);
    }
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  @VisibleForTesting
  public void commit() {
    logStore.commit();
    logStore.refresh();
  }

  // Snapshot methods
  public void preSnapshot() {
    logger.info("Started RW chunk pre-snapshot {}", chunkInfo);
    setReadOnly(true);
    logStore.refresh();
    logStore.finalMerge();
    logStore.commit();
    logStore.refresh();
    logger.info("Finished RW chunk pre-snapshot {}", chunkInfo);
  }

  /** postSnapshot method is called after a snapshot is persisted in a blobstore. */
  public abstract void postSnapshot();

  /**
   * Copy the files from log store to S3 to a given bucket, prefix.
   *
   * @return true on success, false on failure.
   */
  public boolean snapshotToS3(BlobStore blobStore) {
    logger.info("Started RW chunk snapshot to S3 {}", chunkInfo);

    IndexCommit indexCommit = null;
    long totalBytes = 0;
    try {
      Path dirPath = logStore.getDirectory().getDirectory().toAbsolutePath();

      // Create schema file to upload
      ChunkSchema chunkSchema =
          new ChunkSchema(chunkInfo.chunkId, logStore.getSchema(), new ConcurrentHashMap<>());
      File schemaFile = new File(dirPath + "/" + SCHEMA_FILE_NAME);
      ChunkSchema.serializeToFile(chunkSchema, schemaFile);

      // Prepare list of files to upload.
      List<String> filesToUpload = new ArrayList<>();
      filesToUpload.add(schemaFile.getName());
      indexCommit = logStore.getIndexCommit();
      filesToUpload.addAll(indexCommit.getFileNames());
      int numFilesToUpload = filesToUpload.size();

      // Upload files
      logger.info("{} active files in {} in index", filesToUpload.size(), dirPath);
      for (String fileName : filesToUpload) {
        long sizeOfFile = Files.size(Path.of(dirPath + "/" + fileName));
        totalBytes += sizeOfFile;
        logger.debug("File name is {} ({} bytes)", fileName, sizeOfFile);
      }
      this.fileUploadAttempts.increment(filesToUpload.size());
      Timer.Sample snapshotTimer = Timer.start(meterRegistry);

      // blobstore.upload uploads everything in the directory, including write.lock if it exists.
      blobStore.upload(chunkInfo.chunkId, dirPath);

      snapshotTimer.stop(meterRegistry.timer(SNAPSHOT_TIMER));
      chunkInfo.setSizeInBytesOnDisk(totalBytes);

      List<String> filesUploaded = blobStore.listFiles(chunkInfo.chunkId);
      filesUploaded.removeIf(file -> file.endsWith("write.lock"));

      // check here that all files are uploaded
      if (filesUploaded.size() != numFilesToUpload) {
        filesToUpload.sort(String::compareTo);
        filesUploaded.sort(String::compareTo);
        logger.error(
            "Not all files were uploaded to S3. Expected {}, but got {}.\nFiles to upload: {}\nFiles uploaded: {}",
            numFilesToUpload,
            filesUploaded.size(),
            filesToUpload,
            filesUploaded);
        return false;
      }
      // and schema file exists in s3
      if (!filesUploaded.contains(chunkInfo.chunkId + "/" + SCHEMA_FILE_NAME)) {
        logger.error("Schema file was not uploaded to S3: {}", SCHEMA_FILE_NAME);
        return false;
      }
      logger.info("Finished RW chunk snapshot to S3 {}.", chunkInfo);
      return true;
    } catch (Exception e) {
      logger.error("Exception when copying RW chunk " + chunkInfo + " to S3.", e);
      return false;
    } finally {
      logStore.releaseIndexCommit(indexCommit);
    }
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
        query.dataset,
        query.howMany,
        query.queryBuilder,
        query.sourceFieldFilter,
        query.aggregatorFactoriesBuilder);
  }

  @Override
  public Map<String, FieldType> getSchema() {
    return logStore.getSchema().entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().fieldType));
  }
}
