package com.slack.kaldb.chunk;

import static com.slack.kaldb.config.KaldbConfig.CACHE_SLOT_STORE_ZK_PATH;
import static com.slack.kaldb.config.KaldbConfig.REPLICA_STORE_ZK_PATH;
import static com.slack.kaldb.config.KaldbConfig.SEARCH_METADATA_STORE_ZK_PATH;
import static com.slack.kaldb.config.KaldbConfig.SNAPSHOT_METADATA_STORE_ZK_PATH;
import static com.slack.kaldb.logstore.BlobFsUtils.copyFromS3;
import static com.slack.kaldb.metadata.cache.CacheSlotMetadata.METADATA_SLOT_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
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
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
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
  private static final int TIMEOUT_MS = 5000;

  private ChunkInfo chunkInfo;
  private LogIndexSearcher<T> logSearcher;
  private SearchMetadata searchMetadata;

  private final String chunkId;
  private final KaldbConfigs.S3Config s3Config;
  private final SearchContext searchContext;
  protected final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final ReplicaMetadataStore replicaMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final SearchMetadataStore searchMetadataStore;
  private final ExecutorService executorService;
  private final S3BlobFs s3BlobFs;
  private final Path dataDirectory;

  public static final String SUCCESSFUL_CHUNK_ASSIGNMENT = "chunk_assign_success";
  public static final String SUCCESSFUL_CHUNK_EVICTION = "chunk_evict_success";
  public static final String FAILED_CHUNK_ASSIGNMENT = "chunk_assign_fail";
  public static final String FAILED_CHUNK_EVICTION = "chunk_evict_fail";
  protected final Counter successfulChunkAssignments;
  protected final Counter successfulChunkEvictions;
  protected final Counter failedChunkAssignments;
  protected final Counter failedChunkEvictions;

  public ReadOnlyChunkImpl(
      String chunkId,
      MetadataStoreService metadataStoreService,
      KaldbConfigs.KaldbConfig kaldbConfig,
      MeterRegistry meterRegistry,
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
    this.searchContext = SearchContext.fromConfig(kaldbConfig.getCacheConfig().getServerConfig());

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
    searchMetadataStore =
        new SearchMetadataStore(
            metadataStoreService.getMetadataStore(), SEARCH_METADATA_STORE_ZK_PATH, true);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            METADATA_SLOT_NAME, Metadata.CacheSlotState.FREE, "", Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadata);

    Collection<Tag> meterTags = ImmutableList.of(Tag.of("slotId", slotId));
    successfulChunkAssignments = meterRegistry.counter(SUCCESSFUL_CHUNK_ASSIGNMENT, meterTags);
    successfulChunkEvictions = meterRegistry.counter(SUCCESSFUL_CHUNK_EVICTION, meterTags);
    failedChunkAssignments = meterRegistry.counter(FAILED_CHUNK_ASSIGNMENT, meterTags);
    failedChunkEvictions = meterRegistry.counter(FAILED_CHUNK_EVICTION, meterTags);

    LOG.info("Created a new read only chunk {}", chunkId);
  }

  private KaldbMetadataStoreChangeListener cacheNodeListener() {
    return () -> {
      CacheSlotMetadata cacheSlotMetadata = cacheSlotMetadataStore.getCached().get(0);
      LOG.debug("Change on chunk {} - {}", chunkId, cacheSlotMetadata.toString());

      if (cacheSlotMetadata.cacheSlotState.equals(Metadata.CacheSlotState.ASSIGNED)) {
        LOG.info(String.format("Chunk %s - ASSIGNED received", chunkId));
        executorService.submit(() -> handleChunkAssignment(cacheSlotMetadata));
      } else if (cacheSlotMetadata.cacheSlotState.equals(Metadata.CacheSlotState.EVICT)) {
        LOG.info(String.format("Chunk %s - EVICT received", chunkId));
        executorService.submit(this::handleChunkEviction);
      }
    };
  }

  private void registerSearchMetadata(String snapshotName)
      throws ExecutionException, InterruptedException, TimeoutException {
    this.searchMetadata =
        new SearchMetadata(searchContext.hostname, snapshotName, searchContext.toUrl());
    searchMetadataStore.create(searchMetadata).get(5, TimeUnit.SECONDS);
  }

  private void unregisterSearchMetadata()
      throws ExecutionException, InterruptedException, TimeoutException {
    if (this.searchMetadata != null) {
      searchMetadataStore.delete(searchMetadata).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
  }

  private void handleChunkAssignment(CacheSlotMetadata cacheSlotMetadata) {
    try {
      if (!setChunkMetadataState(Metadata.CacheSlotState.LOADING)) {
        throw new InterruptedException("Failed to set chunk metadata state to loading");
      }
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

      if (!setChunkMetadataState(Metadata.CacheSlotState.LIVE)) {
        throw new InterruptedException("Failed to set chunk metadata state to loading");
      }

      registerSearchMetadata(snapshotMetadata.name);

      successfulChunkAssignments.increment();
    } catch (Exception e) {
      failedChunkAssignments.increment();

      // if any error occurs during the chunk assignment, try to release the slot for re-assignment
      // - disregard any errors
      setChunkMetadataState(Metadata.CacheSlotState.FREE);
      LOG.error("Error handling chunk assignment", e);
    }
  }

  private SnapshotMetadata getSnapshotMetadata(String replicaId)
      throws ExecutionException, InterruptedException, TimeoutException {
    ReplicaMetadata replicaMetadata =
        replicaMetadataStore.getNode(replicaId).get(5, TimeUnit.SECONDS);
    return snapshotMetadataStore
        .getNode(replicaMetadata.snapshotId)
        .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
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

  private void handleChunkEviction() {
    try {
      if (!setChunkMetadataState(Metadata.CacheSlotState.EVICTING)) {
        throw new InterruptedException("Failed to set chunk metadata state to evicting");
      }

      // remove this from being query-able
      unregisterSearchMetadata();

      logSearcher.close();
      chunkInfo = null;
      logSearcher = null;

      FileUtils.cleanDirectory(dataDirectory.toFile());
      if (!setChunkMetadataState(Metadata.CacheSlotState.FREE)) {
        throw new InterruptedException("Failed to set chunk metadata state to free");
      }

      successfulChunkEvictions.increment();
    } catch (Exception e) {
      // leave the slot state stuck in evicting, as something is broken, and we don't want a
      // re-assignment or queries hitting this slot
      failedChunkEvictions.increment();
      LOG.error("Error handling chunk eviction", e);
    }
  }

  @VisibleForTesting
  public boolean setChunkMetadataState(Metadata.CacheSlotState newChunkState) {
    CacheSlotMetadata chunkMetadata = cacheSlotMetadataStore.getCached().get(0);
    CacheSlotMetadata updatedChunkMetadata =
        new CacheSlotMetadata(
            chunkMetadata.name,
            newChunkState,
            newChunkState.equals(Metadata.CacheSlotState.FREE) ? "" : chunkMetadata.replicaId,
            Instant.now().toEpochMilli());
    try {
      cacheSlotMetadataStore.update(updatedChunkMetadata).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return true;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Error setting chunk metadata state");
      return false;
    }
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
