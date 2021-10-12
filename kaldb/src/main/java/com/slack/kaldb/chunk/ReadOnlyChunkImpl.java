package com.slack.kaldb.chunk;

import static com.slack.kaldb.config.KaldbConfig.CACHE_SLOT_STORE_ZK_PATH;
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
import java.util.UUID;
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
 * and hydration from the BlobFs, but does not support appending new messages. As events are
 * received from ZK each ReadOnlyChunkImpl will appropriately hydrate or evict a chunk from the
 * BlobFs.
 */
public class ReadOnlyChunkImpl<T> implements Chunk<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyChunkImpl.class);
  private static final int TIMEOUT_MS = 5000;

  private ChunkInfo chunkInfo;
  private LogIndexSearcher<T> logSearcher;
  private SearchMetadata searchMetadata;
  private Path dataDirectory;
  private Metadata.CacheSlotState previousSlotState;

  private final String dataDirectoryPrefix;
  private final String s3Bucket;
  private final SearchContext searchContext;
  protected final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final ReplicaMetadataStore replicaMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final SearchMetadataStore searchMetadataStore;
  private final ExecutorService executorService;
  private final S3BlobFs s3BlobFs;

  public static final String SUCCESSFUL_CHUNK_ASSIGNMENT = "chunk_assign_success";
  public static final String SUCCESSFUL_CHUNK_EVICTION = "chunk_evict_success";
  public static final String FAILED_CHUNK_ASSIGNMENT = "chunk_assign_fail";
  public static final String FAILED_CHUNK_EVICTION = "chunk_evict_fail";
  protected final Counter successfulChunkAssignments;
  protected final Counter successfulChunkEvictions;
  protected final Counter failedChunkAssignments;
  protected final Counter failedChunkEvictions;

  public ReadOnlyChunkImpl(
      MetadataStoreService metadataStoreService,
      MeterRegistry meterRegistry,
      S3BlobFs s3BlobFs,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      SearchMetadataStore searchMetadataStore)
      throws Exception {
    String slotId = UUID.randomUUID().toString();
    this.s3BlobFs = s3BlobFs;
    this.s3Bucket = s3Bucket;
    this.dataDirectoryPrefix = dataDirectoryPrefix;

    // we use a single thread executor to allow operations for this chunk to queue,
    // guaranteeing that they are executed in the order they were received
    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("readonly-chunk-%d").build());
    this.searchContext = searchContext;
    String slotName = String.format("%s-%s", searchContext.hostname, slotId);

    this.replicaMetadataStore = replicaMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.searchMetadataStore = searchMetadataStore;

    // todo - remove the unnecessary additional directory
    String cacheSlotPath = String.format("%s/%s", CACHE_SLOT_STORE_ZK_PATH, slotName);
    cacheSlotMetadataStore =
        new CacheSlotMetadataStore(metadataStoreService.getMetadataStore(), cacheSlotPath, true);
    cacheSlotMetadataStore.addListener(cacheNodeListener());

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            METADATA_SLOT_NAME, Metadata.CacheSlotState.FREE, "", Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadata);
    previousSlotState = Metadata.CacheSlotState.FREE;

    Collection<Tag> meterTags = ImmutableList.of(Tag.of("slotName", slotName));
    successfulChunkAssignments = meterRegistry.counter(SUCCESSFUL_CHUNK_ASSIGNMENT, meterTags);
    successfulChunkEvictions = meterRegistry.counter(SUCCESSFUL_CHUNK_EVICTION, meterTags);
    failedChunkAssignments = meterRegistry.counter(FAILED_CHUNK_ASSIGNMENT, meterTags);
    failedChunkEvictions = meterRegistry.counter(FAILED_CHUNK_EVICTION, meterTags);

    LOG.info("Created a new read only chunk - zkSlotId: {}", slotId);
  }

  private KaldbMetadataStoreChangeListener cacheNodeListener() {
    return () -> {
      CacheSlotMetadata cacheSlotMetadata = cacheSlotMetadataStore.getCached().get(0);
      Metadata.CacheSlotState newSlotState = cacheSlotMetadata.cacheSlotState;

      if (newSlotState.equals(Metadata.CacheSlotState.ASSIGNED)) {
        LOG.info("Chunk - ASSIGNED received");
        if (!previousSlotState.equals(Metadata.CacheSlotState.FREE)) {
          LOG.warn("Unexpected state transition from {} to {}", previousSlotState, newSlotState);
        }
        executorService.submit(() -> handleChunkAssignment(cacheSlotMetadata));
      } else if (newSlotState.equals(Metadata.CacheSlotState.EVICT)) {
        LOG.info("Chunk - EVICT received");
        if (!previousSlotState.equals(Metadata.CacheSlotState.LIVE)) {
          LOG.warn("Unexpected state transition from {} to {}", previousSlotState, newSlotState);
        }
        executorService.submit(this::handleChunkEviction);
      }

      previousSlotState = newSlotState;
    };
  }

  private void registerSearchMetadata(String snapshotName)
      throws ExecutionException, InterruptedException, TimeoutException {
    this.searchMetadata =
        new SearchMetadata(searchContext.hostname, snapshotName, searchContext.toUrl());
    searchMetadataStore.create(searchMetadata).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  private void unregisterSearchMetadata()
      throws ExecutionException, InterruptedException, TimeoutException {
    if (this.searchMetadata != null) {
      searchMetadataStore.delete(searchMetadata).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
  }

  // We synchronize access when manipulating the chunk, as the close() can
  // run concurrently with an assignment
  private synchronized void handleChunkAssignment(CacheSlotMetadata cacheSlotMetadata) {
    try {
      if (!setChunkMetadataState(Metadata.CacheSlotState.LOADING)) {
        throw new InterruptedException("Failed to set chunk metadata state to loading");
      }

      dataDirectory =
          Path.of(
              String.format("%s/kaldb-slot-%s", dataDirectoryPrefix, cacheSlotMetadata.replicaId));
      if (Files.isDirectory(dataDirectory) && Files.list(dataDirectory).findFirst().isPresent()) {
        LOG.warn("Existing files found in slot directory, clearing directory");
        cleanDirectory();
      }

      SnapshotMetadata snapshotMetadata = getSnapshotMetadata(cacheSlotMetadata.replicaId);
      if (copyFromS3(s3Bucket, snapshotMetadata.snapshotId, s3BlobFs, dataDirectory).length == 0) {
        throw new IOException("No files found on blob storage, released slot for re-assignment");
      }

      this.chunkInfo = ChunkInfo.fromSnapshotMetadata(snapshotMetadata, dataDirectory);
      this.logSearcher =
          (LogIndexSearcher<T>)
              new LogIndexSearcherImpl(LogIndexSearcherImpl.searcherManagerFromPath(dataDirectory));

      // we first mark the slot LIVE before registering the search metadata as available
      if (!setChunkMetadataState(Metadata.CacheSlotState.LIVE)) {
        throw new InterruptedException("Failed to set chunk metadata state to loading");
      }

      registerSearchMetadata(snapshotMetadata.name);
      successfulChunkAssignments.increment();
    } catch (Exception e) {
      failedChunkAssignments.increment();

      // if any error occurs during the chunk assignment, try to release the slot for re-assignment,
      // disregarding any errors
      setChunkMetadataState(Metadata.CacheSlotState.FREE);
      LOG.error("Error handling chunk assignment", e);
    }
  }

  private SnapshotMetadata getSnapshotMetadata(String replicaId)
      throws ExecutionException, InterruptedException, TimeoutException {
    ReplicaMetadata replicaMetadata =
        replicaMetadataStore.getNode(replicaId).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    return snapshotMetadataStore
        .getNode(replicaMetadata.snapshotId)
        .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  // We synchronize access when manipulating the chunk, as the close()
  // can run concurrently with an eviction
  private synchronized void handleChunkEviction() {
    try {
      if (!setChunkMetadataState(Metadata.CacheSlotState.EVICTING)) {
        throw new InterruptedException("Failed to set chunk metadata state to evicting");
      }

      // make this chunk un-queryable
      unregisterSearchMetadata();

      if (logSearcher != null) {
        logSearcher.close();
      }

      chunkInfo = null;
      logSearcher = null;

      cleanDirectory();
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

  private void cleanDirectory() {
    if (dataDirectory != null) {
      try {
        FileUtils.cleanDirectory(dataDirectory.toFile());
      } catch (Exception e) {
        LOG.info("Error removing files {}", dataDirectory.toString(), e);
      }
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

  @VisibleForTesting
  public Path getDataDirectory() {
    return dataDirectory;
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
    // Attempt to forcibly shutdown the executor service. This prevents any further downloading of
    // data from S3 that would be unused. We cannot wait for the result of this as there may be
    // many ReadOnlyChunks that all need to be shutdown.
    executorService.shutdownNow();

    // Attempt to evict the chunk
    handleChunkEviction();

    cacheSlotMetadataStore.close();
    LOG.info("Closed chunk");
  }

  @Override
  public String id() {
    if (chunkInfo != null) {
      return chunkInfo.chunkId;
    }
    return null;
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
