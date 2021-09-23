package com.slack.kaldb.chunk;

import static com.slack.kaldb.config.KaldbConfig.CACHE_SLOT_STORE_ZK_PATH;
import static com.slack.kaldb.config.KaldbConfig.REPLICA_STORE_ZK_PATH;
import static com.slack.kaldb.config.KaldbConfig.SNAPSHOT_METADATA_STORE_ZK_PATH;
import static com.slack.kaldb.logstore.BlobFsUtils.copyFromS3;
import static com.slack.kaldb.metadata.cache.CacheSlotMetadata.METADATA_SLOT_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.logstore.search.LogIndexSearcher;
import com.slack.kaldb.logstore.search.LogIndexSearcherImpl;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.metadata.cache.CacheSlotMetadata;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.server.MetadataStoreService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
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
 */
public class ReadOnlyChunkImpl<T> implements Chunk<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyChunkImpl.class);

  private ChunkInfo chunkInfo;
  private LogIndexSearcher<T> logSearcher;

  private final String chunkId;
  private final KaldbConfigs.S3Config s3Config;
  protected final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final ReplicaMetadataStore replicaMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final ExecutorService executorService;
  private final S3BlobFs s3BlobFs;
  private final Path dataDirectory;

  public ReadOnlyChunkImpl(
      String chunkId,
      MetadataStoreService metadataStoreService,
      KaldbConfigs.KaldbConfig kaldbConfig,
      S3BlobFs s3BlobFs)
      throws Exception {
    this.chunkId = chunkId;
    this.s3BlobFs = s3BlobFs;
    this.s3Config = kaldbConfig.getS3Config();
    this.dataDirectory =
        Path.of(String.format("%s/%s", kaldbConfig.getCacheConfig().getDataDirectory(), chunkId));
    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(String.format("readonly-chunk-%s-%%d", chunkId))
                .build());

    String serverAddress = kaldbConfig.getCacheConfig().getServerConfig().getServerAddress();
    String slotId = String.format("%s-%s", serverAddress, chunkId);

    String cacheSlotPath = String.format("%s/%s", CACHE_SLOT_STORE_ZK_PATH, slotId);
    cacheSlotMetadataStore =
        new CacheSlotMetadataStore(metadataStoreService.getMetadataStore(), cacheSlotPath, true);
    cacheSlotMetadataStore.addListener(cacheNodeListener());

    replicaMetadataStore =
        new ReplicaMetadataStore(
            metadataStoreService.getMetadataStore(), REPLICA_STORE_ZK_PATH, false);
    snapshotMetadataStore =
        new SnapshotMetadataStore(
            metadataStoreService.getMetadataStore(), SNAPSHOT_METADATA_STORE_ZK_PATH, false);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            METADATA_SLOT_NAME, Metadata.CacheSlotState.FREE, "", Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadata);

    LOG.info("Created a new read only chunk {}", chunkId);
  }

  private KaldbMetadataStoreChangeListener cacheNodeListener() {
    return () -> {
      CacheSlotMetadata cacheSlotMetadata = cacheSlotMetadataStore.getCached().get(0);
      LOG.debug("Change on chunk {} - {}", chunkId, cacheSlotMetadata.toString());

      if (cacheSlotMetadata.cacheSlotState.equals(Metadata.CacheSlotState.ASSIGNED)) {
        LOG.info(String.format("Chunk %s - ASSIGNED received", chunkId));
        executorService.submit(
            () -> {
              try {
                handleChunkAssignment(cacheSlotMetadata);
              } catch (Exception e) {
                LOG.error("Error handling chunk assignment", e);
              }
            });
      } else if (cacheSlotMetadata.cacheSlotState.equals(Metadata.CacheSlotState.EVICT)) {
        LOG.info(String.format("Chunk %s - EVICT received", chunkId));
        executorService.submit(
            () -> {
              try {
                handleChunkEviction();
              } catch (Exception e) {
                LOG.error("Error handling chunk eviction", e);
              }
            });
      }
    };
  }

  private void handleChunkAssignment(CacheSlotMetadata cacheSlotMetadata) throws Exception {
    try {
      setChunkMetadataState(Metadata.CacheSlotState.LOADING);
      SnapshotMetadata snapshotMetadata = getSnapshotMetadata(cacheSlotMetadata.replicaId);

      // todo - verify this - chunkId == prefix == snapshotId
      String chunkId, prefix = snapshotMetadata.snapshotId;

      if (copyFromS3(s3Config.getS3Bucket(), prefix, s3BlobFs, dataDirectory).length == 0) {
        throw new IOException("No files found on blob storage, released slot for re-assignment");
      }

      this.chunkInfo = getChunkInfo(snapshotMetadata, dataDirectory);
      this.logSearcher =
          (LogIndexSearcher<T>)
              new LogIndexSearcherImpl(LogIndexSearcherImpl.searcherManagerFromPath(dataDirectory));

      setChunkMetadataState(Metadata.CacheSlotState.LIVE);
    } catch (Exception e) {
      // if any error occurs during the chunk assignment, ensure we release the slot
      setChunkMetadataState(Metadata.CacheSlotState.FREE);
      throw e;
    }
  }

  private SnapshotMetadata getSnapshotMetadata(String replicaId)
      throws ExecutionException, InterruptedException, TimeoutException {
    ReplicaMetadata replicaMetadata =
        replicaMetadataStore.getNode(replicaId).get(5, TimeUnit.SECONDS);
    return snapshotMetadataStore.getNode(replicaMetadata.snapshotId).get(5, TimeUnit.SECONDS);
  }

  private ChunkInfo getChunkInfo(SnapshotMetadata snapshotMetadata, Path dataDirectory) {
    ChunkInfo chunkInfo = new ChunkInfo(chunkId, Instant.now().toEpochMilli());

    chunkInfo.setDataStartTimeEpochMs(snapshotMetadata.startTimeUtc);
    chunkInfo.setDataEndTimeEpochMs(snapshotMetadata.endTimeUtc);

    // todo - do we need to set these values?
    chunkInfo.setChunkLastUpdatedTimeEpochMs(snapshotMetadata.endTimeUtc);
    chunkInfo.setChunkSnapshotTimeEpochMs(snapshotMetadata.endTimeUtc);
    try {
      chunkInfo.setChunkSize(Files.size(dataDirectory));
      chunkInfo.setNumDocs(LogIndexSearcherImpl.getNumDocs(dataDirectory));
    } catch (IOException ignored) {
    }
    return chunkInfo;
  }

  private void handleChunkEviction() throws Exception {
    setChunkMetadataState(Metadata.CacheSlotState.EVICTING);

    chunkInfo = null;

    logSearcher.close();
    logSearcher = null;

    FileUtils.cleanDirectory(dataDirectory.toFile());
    setChunkMetadataState(Metadata.CacheSlotState.FREE);
  }

  @VisibleForTesting
  public void setChunkMetadataState(Metadata.CacheSlotState newChunkState) throws Exception {
    CacheSlotMetadata chunkMetadata = cacheSlotMetadataStore.getCached().get(0);
    CacheSlotMetadata updatedChunkMetadata =
        new CacheSlotMetadata(
            chunkMetadata.name,
            newChunkState,
            newChunkState.equals(Metadata.CacheSlotState.FREE) ? "" : chunkMetadata.replicaId,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.update(updatedChunkMetadata).get(5, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  public Metadata.CacheSlotState getChunkMetadataState() {
    if (cacheSlotMetadataStore.getCached().isEmpty()) {
      return null;
    }
    return cacheSlotMetadataStore.getCached().get(0).cacheSlotState;
  }

  @Override
  public ChunkInfo info() {
    return chunkInfo;
  }

  @Override
  public boolean containsDataInTimeRange(long startTs, long endTs) {
    if (chunkInfo != null) {
      return chunkInfo.containsDataInTimeRange(startTs, endTs);
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    if (logSearcher != null) {
      logSearcher.close();
    }

    executorService.shutdown();
    cacheSlotMetadataStore.close();

    LOG.info("Closed chunk {}", chunkId);
    cleanup();
  }

  /** Deletes the log store data from local disk. */
  public void cleanup() {
    // TODO: Implement chunk state cleanup
  }

  @Override
  public String id() {
    return chunkId;
  }

  @Override
  public SearchResult<T> query(SearchQuery query) {
    if (logSearcher != null) {
      return logSearcher.search(
          query.indexName,
          query.queryStr,
          query.startTimeEpochMs,
          query.endTimeEpochMs,
          query.howMany,
          query.bucketCount);
    } else {
      return SearchResult.empty();
    }
  }
}
