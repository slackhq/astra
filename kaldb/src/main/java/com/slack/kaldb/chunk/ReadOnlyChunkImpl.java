package com.slack.kaldb.chunk;

import com.google.common.annotations.VisibleForTesting;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.logstore.search.LogIndexSearcher;
import com.slack.kaldb.logstore.search.LogIndexSearcherImpl;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.metadata.cache.CacheSlotMetadata;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.schema.ChunkSchema;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.curator.x.async.AsyncCuratorFramework;
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

  @Deprecated // replace with sync methods, which use DEFAULT_ZK_TIMEOUT_SECS where possible
  private static final int TIMEOUT_MS = 5000;

  private ChunkInfo chunkInfo;
  private LogIndexSearcher<T> logSearcher;
  private SearchMetadata searchMetadata;
  private Path dataDirectory;
  private ChunkSchema chunkSchema;
  private Metadata.CacheSlotMetadata.CacheSlotState cacheSlotLastKnownState;

  private final String dataDirectoryPrefix;
  private final String s3Bucket;
  protected final SearchContext searchContext;
  protected final String slotId;
  private final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final ReplicaMetadataStore replicaMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final SearchMetadataStore searchMetadataStore;
  private final MeterRegistry meterRegistry;

  private final ExecutorService executorService;
  private final BlobFs blobFs;

  public static final String CHUNK_ASSIGNMENT_TIMER = "chunk_assignment_timer";
  public static final String CHUNK_EVICTION_TIMER = "chunk_eviction_timer";

  private final Timer chunkAssignmentTimerSuccess;
  private final Timer chunkAssignmentTimerFailure;
  private final Timer chunkEvictionTimerSuccess;
  private final Timer chunkEvictionTimerFailure;

  private final KaldbMetadataStoreChangeListener<CacheSlotMetadata> cacheSlotListener =
      this::cacheNodeListener;

  public ReadOnlyChunkImpl(
      AsyncCuratorFramework curatorFramework,
      MeterRegistry meterRegistry,
      BlobFs blobFs,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      CacheSlotMetadataStore cacheSlotMetadataStore,
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      SearchMetadataStore searchMetadataStore,
      ExecutorService executorService)
      throws Exception {
    this.meterRegistry = meterRegistry;
    this.blobFs = blobFs;
    this.s3Bucket = s3Bucket;
    this.dataDirectoryPrefix = dataDirectoryPrefix;
    this.executorService = executorService;
    this.searchContext = searchContext;
    this.slotId = UUID.randomUUID().toString();

    this.cacheSlotMetadataStore = cacheSlotMetadataStore;
    this.replicaMetadataStore = replicaMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.searchMetadataStore = searchMetadataStore;

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            slotId,
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            List.of(Metadata.IndexType.LOGS_LUCENE9),
            searchContext.hostname);
    cacheSlotMetadataStore.createSync(cacheSlotMetadata);
    cacheSlotMetadataStore.addListener(cacheSlotListener);

    cacheSlotLastKnownState = Metadata.CacheSlotMetadata.CacheSlotState.FREE;
    chunkAssignmentTimerSuccess = meterRegistry.timer(CHUNK_ASSIGNMENT_TIMER, "successful", "true");
    chunkAssignmentTimerFailure =
        meterRegistry.timer(CHUNK_ASSIGNMENT_TIMER, "successful", "false");
    chunkEvictionTimerSuccess = meterRegistry.timer(CHUNK_EVICTION_TIMER, "successful", "true");
    chunkEvictionTimerFailure = meterRegistry.timer(CHUNK_EVICTION_TIMER, "successful", "false");

    LOG.info("Created a new read only chunk - zkSlotId: {}", slotId);
  }

  private void cacheNodeListener(CacheSlotMetadata cacheSlotMetadata) {
    if (Objects.equals(cacheSlotMetadata.name, slotId)) {
      Metadata.CacheSlotMetadata.CacheSlotState newSlotState = cacheSlotMetadata.cacheSlotState;

      if (newSlotState.equals(Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED)) {
        LOG.info("Chunk - ASSIGNED received");
        if (!cacheSlotLastKnownState.equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE)) {
          LOG.warn(
              "Unexpected state transition from {} to {}", cacheSlotLastKnownState, newSlotState);
        }
        executorService.execute(() -> handleChunkAssignment(cacheSlotMetadata));
      } else if (newSlotState.equals(Metadata.CacheSlotMetadata.CacheSlotState.EVICT)) {
        LOG.info("Chunk - EVICT received");
        if (!cacheSlotLastKnownState.equals(Metadata.CacheSlotMetadata.CacheSlotState.LIVE)) {
          LOG.warn(
              "Unexpected state transition from {} to {}", cacheSlotLastKnownState, newSlotState);
        }
        executorService.execute(this::handleChunkEviction);
      }

      cacheSlotLastKnownState = newSlotState;
    }
  }

  @VisibleForTesting
  public static SearchMetadata registerSearchMetadata(
      SearchMetadataStore searchMetadataStore,
      SearchContext cacheSearchContext,
      String snapshotName)
      throws ExecutionException, InterruptedException, TimeoutException {
    SearchMetadata metadata =
        new SearchMetadata(
            SearchMetadata.generateSearchContextSnapshotId(
                snapshotName, cacheSearchContext.hostname),
            snapshotName,
            cacheSearchContext.toUrl());
    searchMetadataStore
        .createAsync(metadata)
        .toCompletableFuture()
        .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    return metadata;
  }

  private void unregisterSearchMetadata()
      throws ExecutionException, InterruptedException, TimeoutException {
    if (this.searchMetadata != null) {
      searchMetadataStore
          .deleteAsync(searchMetadata)
          .toCompletableFuture()
          .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
  }

  // We synchronize access when manipulating the chunk, as the close() can
  // run concurrently with an assignment
  private synchronized void handleChunkAssignment(CacheSlotMetadata cacheSlotMetadata) {
    Timer.Sample assignmentTimer = Timer.start(meterRegistry);
    try {
      if (!setChunkMetadataState(
          searchContext.hostname, slotId, Metadata.CacheSlotMetadata.CacheSlotState.LOADING)) {
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
      SerialS3ChunkDownloaderImpl chunkDownloader =
          new SerialS3ChunkDownloaderImpl(
              s3Bucket, snapshotMetadata.snapshotId, blobFs, dataDirectory);
      if (chunkDownloader.download()) {
        throw new IOException("No files found on blob storage, released slot for re-assignment");
      }

      Path schemaPath = Path.of(dataDirectory.toString(), ReadWriteChunk.SCHEMA_FILE_NAME);
      if (!Files.exists(schemaPath)) {
        throw new RuntimeException("We expect a schema.json file to exist within the index");
      }
      this.chunkSchema = ChunkSchema.deserializeFile(schemaPath);

      this.chunkInfo = ChunkInfo.fromSnapshotMetadata(snapshotMetadata);
      this.logSearcher =
          (LogIndexSearcher<T>)
              new LogIndexSearcherImpl(
                  LogIndexSearcherImpl.searcherManagerFromPath(dataDirectory),
                  chunkSchema.fieldDefMap);

      // we first mark the slot LIVE before registering the search metadata as available
      if (!setChunkMetadataState(
          searchContext.hostname, slotId, Metadata.CacheSlotMetadata.CacheSlotState.LIVE)) {
        throw new InterruptedException("Failed to set chunk metadata state to loading");
      }

      searchMetadata =
          registerSearchMetadata(searchMetadataStore, searchContext, snapshotMetadata.name);
      assignmentTimer.stop(chunkAssignmentTimerSuccess);
    } catch (Exception e) {
      // if any error occurs during the chunk assignment, try to release the slot for re-assignment,
      // disregarding any errors
      setChunkMetadataState(
          searchContext.hostname, slotId, Metadata.CacheSlotMetadata.CacheSlotState.FREE);
      LOG.error("Error handling chunk assignment", e);
      assignmentTimer.stop(chunkAssignmentTimerFailure);
    }
  }

  private SnapshotMetadata getSnapshotMetadata(String replicaId)
      throws ExecutionException, InterruptedException, TimeoutException {
    ReplicaMetadata replicaMetadata =
        replicaMetadataStore
            .findAsync(replicaId)
            .toCompletableFuture()
            .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    return snapshotMetadataStore
        .findAsync(replicaMetadata.snapshotId)
        .toCompletableFuture()
        .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  // We synchronize access when manipulating the chunk, as the close()
  // can run concurrently with an eviction
  private synchronized void handleChunkEviction() {
    Timer.Sample evictionTimer = Timer.start(meterRegistry);
    try {
      if (!setChunkMetadataState(
          searchContext.hostname, slotId, Metadata.CacheSlotMetadata.CacheSlotState.EVICTING)) {
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
      if (!setChunkMetadataState(
          searchContext.hostname, slotId, Metadata.CacheSlotMetadata.CacheSlotState.FREE)) {
        throw new InterruptedException("Failed to set chunk metadata state to free");
      }

      evictionTimer.stop(chunkEvictionTimerSuccess);
    } catch (Exception e) {
      // leave the slot state stuck in evicting, as something is broken, and we don't want a
      // re-assignment or queries hitting this slot
      LOG.error("Error handling chunk eviction", e);
      evictionTimer.stop(chunkEvictionTimerFailure);
    }
  }

  /** Fetch the node given a slotName and update the slot state. */
  private boolean setChunkMetadataState(
      String partition, String slotName, Metadata.CacheSlotMetadata.CacheSlotState slotState) {
    try {
      cacheSlotMetadataStore
          .getAndUpdateNonFreeCacheSlotState(partition, slotName, slotState)
          .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return true;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Error setting chunk metadata state", e);
      return false;
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
  public Metadata.CacheSlotMetadata.CacheSlotState getChunkMetadataState() {
    return cacheSlotMetadataStore.getSync(searchContext.hostname, slotId).cacheSlotState;
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
  public Map<String, FieldType> getSchema() {
    if (chunkSchema != null) {
      return chunkSchema.fieldDefMap.entrySet().stream()
          .collect(
              Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().fieldType));
    } else {
      return Map.of();
    }
  }

  @Override
  public void close() throws IOException {
    if (cacheSlotMetadataStore.getSync(searchContext.hostname, slotId).cacheSlotState
        != Metadata.CacheSlotMetadata.CacheSlotState.FREE) {
      // Attempt to evict the chunk
      handleChunkEviction();
    }
    cacheSlotMetadataStore.removeListener(cacheSlotListener);
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
          query.dataset,
          query.queryStr,
          query.startTimeEpochMs,
          query.endTimeEpochMs,
          query.howMany,
          query.aggBuilder);
    } else {
      return (SearchResult<T>) SearchResult.empty();
    }
  }
}
