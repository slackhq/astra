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
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
  private Metadata.CacheSlotMetadata.CacheSlotState cacheSlotLastKnownState;

  private final String dataDirectoryPrefix;
  private final String s3Bucket;
  private final SearchContext searchContext;
  protected final String slotName;
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

  public ReadOnlyChunkImpl(
      MetadataStore metadataStore,
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
    String slotId = UUID.randomUUID().toString();
    this.meterRegistry = meterRegistry;
    this.blobFs = blobFs;
    this.s3Bucket = s3Bucket;
    this.dataDirectoryPrefix = dataDirectoryPrefix;
    this.executorService = executorService;
    this.searchContext = searchContext;
    this.slotName = String.format("%s-%s", searchContext.hostname, slotId);

    this.cacheSlotMetadataStore = cacheSlotMetadataStore;
    this.replicaMetadataStore = replicaMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.searchMetadataStore = searchMetadataStore;

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            slotName,
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.createSync(cacheSlotMetadata);

    CacheSlotMetadataStore cacheSlotListenerMetadataStore =
        new CacheSlotMetadataStore(metadataStore, slotName, true);
    cacheSlotListenerMetadataStore.addListener(cacheNodeListener());
    cacheSlotLastKnownState = Metadata.CacheSlotMetadata.CacheSlotState.FREE;

    chunkAssignmentTimerSuccess = meterRegistry.timer(CHUNK_ASSIGNMENT_TIMER, "successful", "true");
    chunkAssignmentTimerFailure =
        meterRegistry.timer(CHUNK_ASSIGNMENT_TIMER, "successful", "false");
    chunkEvictionTimerSuccess = meterRegistry.timer(CHUNK_EVICTION_TIMER, "successful", "true");
    chunkEvictionTimerFailure = meterRegistry.timer(CHUNK_EVICTION_TIMER, "successful", "false");

    LOG.info("Created a new read only chunk - zkSlotId: {}", slotId);
  }

  private KaldbMetadataStoreChangeListener cacheNodeListener() {
    return () -> {
      CacheSlotMetadata cacheSlotMetadata = cacheSlotMetadataStore.getNodeSync(slotName);
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
    };
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
    searchMetadataStore.create(metadata).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    return metadata;
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
    Timer.Sample assignmentTimer = Timer.start(meterRegistry);
    try {
      if (!setChunkMetadataState(Metadata.CacheSlotMetadata.CacheSlotState.LOADING)) {
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

      this.chunkInfo = ChunkInfo.fromSnapshotMetadata(snapshotMetadata);
      this.logSearcher =
          (LogIndexSearcher<T>)
              new LogIndexSearcherImpl(LogIndexSearcherImpl.searcherManagerFromPath(dataDirectory));

      // we first mark the slot LIVE before registering the search metadata as available
      if (!setChunkMetadataState(Metadata.CacheSlotMetadata.CacheSlotState.LIVE)) {
        throw new InterruptedException("Failed to set chunk metadata state to loading");
      }

      searchMetadata =
          registerSearchMetadata(searchMetadataStore, searchContext, snapshotMetadata.name);
      assignmentTimer.stop(chunkAssignmentTimerSuccess);
    } catch (Exception e) {
      // if any error occurs during the chunk assignment, try to release the slot for re-assignment,
      // disregarding any errors
      setChunkMetadataState(Metadata.CacheSlotMetadata.CacheSlotState.FREE);
      LOG.error("Error handling chunk assignment", e);
      assignmentTimer.stop(chunkAssignmentTimerFailure);
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
    Timer.Sample evictionTimer = Timer.start(meterRegistry);
    try {
      if (!setChunkMetadataState(Metadata.CacheSlotMetadata.CacheSlotState.EVICTING)) {
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
      if (!setChunkMetadataState(Metadata.CacheSlotMetadata.CacheSlotState.FREE)) {
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
  public boolean setChunkMetadataState(Metadata.CacheSlotMetadata.CacheSlotState newChunkState) {
    CacheSlotMetadata chunkMetadata = cacheSlotMetadataStore.getNodeSync(slotName);
    CacheSlotMetadata updatedChunkMetadata =
        new CacheSlotMetadata(
            chunkMetadata.name,
            newChunkState,
            newChunkState.equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE)
                ? ""
                : chunkMetadata.replicaId,
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
  public Metadata.CacheSlotMetadata.CacheSlotState getChunkMetadataState() {
    return cacheSlotMetadataStore.getNodeSync(slotName).cacheSlotState;
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
          query.dataset,
          query.queryStr,
          query.startTimeEpochMs,
          query.endTimeEpochMs,
          query.howMany,
          query.searchAggregations);
    } else {
      return (SearchResult<T>) SearchResult.empty();
    }
  }
}
