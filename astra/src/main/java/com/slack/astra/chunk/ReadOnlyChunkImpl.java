package com.slack.astra.chunk;

import static com.slack.astra.chunkManager.CachingChunkManager.ASTRA_NG_DYNAMIC_CHUNK_SIZES_FLAG;
import static com.slack.astra.server.AstraConfig.DEFAULT_ZK_TIMEOUT_SECS;

import com.google.common.annotations.VisibleForTesting;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.logstore.search.AstraSearcherManager;
import com.slack.astra.logstore.search.LogIndexSearcher;
import com.slack.astra.logstore.search.LogIndexSearcherImpl;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.cache.CacheSlotMetadata;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.replica.ReplicaMetadata;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.schema.ChunkSchema;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  public static final String ASTRA_S3_STREAMING_FLAG = "astra.s3Streaming.enabled";

  private ChunkInfo chunkInfo;
  private LogIndexSearcher<T> logSearcher;
  private SearchMetadata searchMetadata;
  private Path dataDirectory;
  private ChunkSchema chunkSchema;
  private CacheNodeAssignment assignment;
  private SnapshotMetadata snapshotMetadata;
  private Metadata.CacheSlotMetadata.CacheSlotState cacheSlotLastKnownState;
  private Metadata.CacheNodeAssignment.CacheNodeAssignmentState lastKnownAssignmentState;

  private final String dataDirectoryPrefix;
  protected final SearchContext searchContext;
  protected final String slotId;
  private final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final ReplicaMetadataStore replicaMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final SearchMetadataStore searchMetadataStore;
  private CacheNodeAssignmentStore cacheNodeAssignmentStore;
  private final MeterRegistry meterRegistry;
  private final BlobStore blobStore;

  public static final String CHUNK_ASSIGNMENT_TIMER = "chunk_assignment_timer";
  public static final String CHUNK_EVICTION_TIMER = "chunk_eviction_timer";

  private final Timer chunkAssignmentTimerSuccess;
  private final Timer chunkAssignmentTimerFailure;
  private final Timer chunkEvictionTimerSuccess;
  private final Timer chunkEvictionTimerFailure;
  private final CacheNodeMetadataStore cacheNodeMetadataStore;

  private final AstraMetadataStoreChangeListener<CacheSlotMetadata> cacheSlotListener =
      this::cacheNodeListener;

  private final ReentrantLock chunkAssignmentLock = new ReentrantLock();

  private static final boolean USE_S3_STREAMING =
      Boolean.parseBoolean(System.getProperty(ASTRA_S3_STREAMING_FLAG, "false"));

  public ReadOnlyChunkImpl(
      AsyncCuratorFramework curatorFramework,
      MeterRegistry meterRegistry,
      BlobStore blobStore,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      String replicaSet,
      CacheSlotMetadataStore cacheSlotMetadataStore,
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      SearchMetadataStore searchMetadataStore,
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      CacheNodeAssignment assignment,
      SnapshotMetadata snapshotMetadata,
      CacheNodeMetadataStore cacheNodeMetadataStore)
      throws Exception {
    this(
        curatorFramework,
        meterRegistry,
        blobStore,
        searchContext,
        s3Bucket,
        dataDirectoryPrefix,
        replicaSet,
        cacheSlotMetadataStore,
        replicaMetadataStore,
        snapshotMetadataStore,
        searchMetadataStore,
        cacheNodeMetadataStore);
    this.assignment = assignment;
    this.lastKnownAssignmentState = assignment.state;
    this.snapshotMetadata = snapshotMetadata;
    this.cacheNodeAssignmentStore = cacheNodeAssignmentStore;
  }

  public ReadOnlyChunkImpl(
      AsyncCuratorFramework curatorFramework,
      MeterRegistry meterRegistry,
      BlobStore blobStore,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      String replicaSet,
      CacheSlotMetadataStore cacheSlotMetadataStore,
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      SearchMetadataStore searchMetadataStore,
      CacheNodeMetadataStore cacheNodeMetadataStore)
      throws Exception {
    this.meterRegistry = meterRegistry;
    this.blobStore = blobStore;
    this.dataDirectoryPrefix = dataDirectoryPrefix;
    this.searchContext = searchContext;
    this.slotId = UUID.randomUUID().toString();

    this.cacheSlotMetadataStore = cacheSlotMetadataStore;
    this.replicaMetadataStore = replicaMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.searchMetadataStore = searchMetadataStore;
    this.cacheNodeMetadataStore = cacheNodeMetadataStore;

    if (!Boolean.getBoolean(ASTRA_NG_DYNAMIC_CHUNK_SIZES_FLAG)) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              slotId,
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              searchContext.hostname,
              replicaSet);
      cacheSlotMetadataStore.createSync(cacheSlotMetadata);
      cacheSlotMetadataStore.addListener(cacheSlotListener);
    }

    cacheSlotLastKnownState = Metadata.CacheSlotMetadata.CacheSlotState.FREE;
    chunkAssignmentTimerSuccess = meterRegistry.timer(CHUNK_ASSIGNMENT_TIMER, "successful", "true");
    chunkAssignmentTimerFailure =
        meterRegistry.timer(CHUNK_ASSIGNMENT_TIMER, "successful", "false");
    chunkEvictionTimerSuccess = meterRegistry.timer(CHUNK_EVICTION_TIMER, "successful", "true");
    chunkEvictionTimerFailure = meterRegistry.timer(CHUNK_EVICTION_TIMER, "successful", "false");

    LOG.debug("Created a new read only chunk - zkSlotId: {}", slotId);
  }

  /*
  ======================================================
  All methods below RELATED to astra.ng.dynamicChunkSizes
  ======================================================
   */

  public void evictChunk(CacheNodeAssignment cacheNodeAssignment) {
    Timer.Sample evictionTimer = Timer.start(meterRegistry);
    chunkAssignmentLock.lock();
    try {
      if (!setAssignmentState(
          cacheNodeAssignment, Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICTING)) {
        throw new InterruptedException("Failed to set cache node assignment state to evicting");
      }
      lastKnownAssignmentState = Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICTING;

      // make this chunk un-queryable
      unregisterSearchMetadata();

      if (logSearcher != null) {
        logSearcher.close();
      }

      chunkInfo = null;
      logSearcher = null;

      cleanDirectory();

      // delete assignment
      cacheNodeAssignmentStore.deleteSync(cacheNodeAssignment);

      evictionTimer.stop(chunkEvictionTimerSuccess);
    } catch (Exception e) {
      // leave the slot state stuck in evicting, as something is broken, and we don't want a
      // re-assignment or queries hitting this slot
      LOG.error("Error handling chunk eviction", e);
      evictionTimer.stop(chunkEvictionTimerFailure);
    } finally {
      chunkAssignmentLock.unlock();
    }
  }

  public CacheNodeAssignment getCacheNodeAssignment() {
    return assignment;
  }

  public void downloadChunkData() {
    Timer.Sample assignmentTimer = Timer.start(meterRegistry);
    // lock
    chunkAssignmentLock.lock();
    try {
      CacheNodeAssignment assignment = getCacheNodeAssignment();
      this.chunkInfo = ChunkInfo.fromSnapshotMetadata(snapshotMetadata);

      if (USE_S3_STREAMING) {
        this.chunkSchema = ChunkSchema.deserializeBytes(blobStore.getSchema(chunkInfo.chunkId));
        this.logSearcher =
            (LogIndexSearcher<T>)
                new LogIndexSearcherImpl(
                    new AstraSearcherManager(chunkInfo.chunkId, blobStore),
                    chunkSchema.fieldDefMap);
      } else {
        // get data directory
        dataDirectory =
            Path.of(
                String.format("%s/astra-chunk-%s", dataDirectoryPrefix, assignment.assignmentId));

        if (Files.isDirectory(dataDirectory)) {
          try (Stream<Path> files = Files.list(dataDirectory)) {
            if (files.findFirst().isPresent()) {
              LOG.warn("Existing files found in slot directory, clearing directory");
              cleanDirectory();
            }
          }
        }

        blobStore.download(snapshotMetadata.snapshotId, dataDirectory);
        try (Stream<Path> fileList = Files.list(dataDirectory)) {
          if (fileList.findAny().isEmpty()) {
            throw new IOException(
                "No files found on blob storage, released slot for re-assignment");
          }
        }

        Path schemaPath = Path.of(dataDirectory.toString(), ReadWriteChunk.SCHEMA_FILE_NAME);
        if (!Files.exists(schemaPath)) {
          throw new RuntimeException("We expect a schema.json file to exist within the index");
        }
        this.chunkSchema = ChunkSchema.deserializeFile(schemaPath);

        this.logSearcher =
            (LogIndexSearcher<T>)
                new LogIndexSearcherImpl(
                    new AstraSearcherManager(dataDirectory), chunkSchema.fieldDefMap);
      }

      // set chunk state
      cacheNodeAssignmentStore.updateAssignmentState(
          getCacheNodeAssignment(), Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE);
      lastKnownAssignmentState = Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE;

      // register searchmetadata
      searchMetadata =
          registerSearchMetadata(searchMetadataStore, searchContext, snapshotMetadata.name);
      long durationNanos = assignmentTimer.stop(chunkAssignmentTimerSuccess);

      if (USE_S3_STREAMING) {
        LOG.info(
            "Downloaded chunk with snapshot id '{}' in {} seconds, astra.cache.s3streaming is enabled",
            snapshotMetadata.snapshotId,
            TimeUnit.SECONDS.convert(durationNanos, TimeUnit.NANOSECONDS));
      } else {
        LOG.info(
            "Downloaded chunk with snapshot id '{}' in {} seconds, was {}",
            snapshotMetadata.snapshotId,
            TimeUnit.SECONDS.convert(durationNanos, TimeUnit.NANOSECONDS),
            FileUtils.byteCountToDisplaySize(FileUtils.sizeOfDirectory(dataDirectory.toFile())));
      }
    } catch (Exception e) {
      // if any error occurs during the chunk assignment, try to release the slot for re-assignment,
      // disregarding any errors
      setAssignmentState(
          getCacheNodeAssignment(), Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT);
      LOG.error("Error handling chunk assignment", e);
      assignmentTimer.stop(chunkAssignmentTimerFailure);
    } finally {
      chunkAssignmentLock.unlock();
    }
  }

  private boolean setAssignmentState(
      CacheNodeAssignment cacheNodeAssignment,
      Metadata.CacheNodeAssignment.CacheNodeAssignmentState newState) {
    try {
      cacheNodeAssignmentStore
          .updateAssignmentState(cacheNodeAssignment, newState)
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
      return true;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Error setting cache node assignment metadata state", e);
      return false;
    }
  }

  public Metadata.CacheNodeAssignment.CacheNodeAssignmentState getLastKnownAssignmentState() {
    return lastKnownAssignmentState;
  }

  /*
  ======================================================
  All methods below UNRELATED to astra.ng.dynamicChunkSizes
  ======================================================
   */
  private void cacheNodeListener(CacheSlotMetadata cacheSlotMetadata) {
    if (Objects.equals(cacheSlotMetadata.name, slotId)) {
      Metadata.CacheSlotMetadata.CacheSlotState newSlotState = cacheSlotMetadata.cacheSlotState;
      if (newSlotState != cacheSlotLastKnownState) {
        if (newSlotState.equals(Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED)) {
          LOG.info("Chunk - ASSIGNED received - {}", cacheSlotMetadata);
          if (!cacheSlotLastKnownState.equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE)) {
            LOG.warn(
                "Unexpected state transition from {} to {} - {}",
                cacheSlotLastKnownState,
                newSlotState,
                cacheSlotMetadata);
          }
          Thread.ofVirtual().start(() -> handleChunkAssignment(cacheSlotMetadata));
        } else if (newSlotState.equals(Metadata.CacheSlotMetadata.CacheSlotState.EVICT)) {
          LOG.info("Chunk - EVICT received - {}", cacheSlotMetadata);
          if (!EnumSet.of(
                  Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
                  Metadata.CacheSlotMetadata.CacheSlotState.LOADING)
              .contains(cacheSlotLastKnownState)) {
            LOG.warn(
                "Unexpected state transition from {} to {} - {}",
                cacheSlotLastKnownState,
                newSlotState,
                cacheSlotMetadata);
          }
          Thread.ofVirtual().start(() -> handleChunkEviction(cacheSlotMetadata));
        }
        cacheSlotLastKnownState = newSlotState;
      } else {
        LOG.debug("Cache node listener fired but slot state was the same - {}", cacheSlotMetadata);
      }
    }
  }

  private void unregisterSearchMetadata()
      throws ExecutionException, InterruptedException, TimeoutException {
    if (this.searchMetadata != null) {
      searchMetadataStore.deleteSync(searchMetadata);
    }
  }

  private SnapshotMetadata getSnapshotMetadata(String replicaId)
      throws ExecutionException, InterruptedException, TimeoutException {
    ReplicaMetadata replicaMetadata = replicaMetadataStore.findSync(replicaId);
    return snapshotMetadataStore.findSync(replicaMetadata.snapshotId);
  }

  public String getSlotId() {
    return slotId;
  }

  private void handleChunkAssignment(CacheSlotMetadata cacheSlotMetadata) {
    Timer.Sample assignmentTimer = Timer.start(meterRegistry);
    chunkAssignmentLock.lock();
    try {
      if (!setChunkMetadataState(
          cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.LOADING)) {
        throw new InterruptedException("Failed to set chunk metadata state to loading");
      }

      dataDirectory =
          Path.of(
              String.format("%s/astra-slot-%s", dataDirectoryPrefix, cacheSlotMetadata.replicaId));

      if (Files.isDirectory(dataDirectory)) {
        try (Stream<Path> files = Files.list(dataDirectory)) {
          if (files.findFirst().isPresent()) {
            LOG.warn("Existing files found in slot directory, clearing directory");
            cleanDirectory();
          }
        }
      }

      SnapshotMetadata snapshotMetadata = getSnapshotMetadata(cacheSlotMetadata.replicaId);
      blobStore.download(snapshotMetadata.snapshotId, dataDirectory);
      try (Stream<Path> fileList = Files.list(dataDirectory)) {
        long numFilesLocal = fileList.count();
        if (numFilesLocal == 0) {
          throw new IOException("No files found on blob storage, released slot for re-assignment");
        }

        List<String> filesInS3 = blobStore.listFiles(snapshotMetadata.snapshotId);
        if (numFilesLocal != filesInS3.size()) {
          String errorString =
              String.format(
                  "Mismatch in number of files in S3 (%s) and local directory (%s) for snapshot %s",
                  filesInS3.size(), numFilesLocal, snapshotMetadata.toString());
          LOG.error(errorString);
          throw new IOException(errorString);
        }
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
                  new AstraSearcherManager(dataDirectory), chunkSchema.fieldDefMap);

      // we first mark the slot LIVE before registering the search metadata as available
      if (!setChunkMetadataState(
          cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.LIVE)) {
        throw new InterruptedException("Failed to set chunk metadata state to loading");
      }

      searchMetadata =
          registerSearchMetadata(searchMetadataStore, searchContext, snapshotMetadata.name);
      long durationNanos = assignmentTimer.stop(chunkAssignmentTimerSuccess);

      LOG.debug(
          "Downloaded chunk with snapshot id '{}' in {} seconds, was {}",
          snapshotMetadata.snapshotId,
          TimeUnit.SECONDS.convert(durationNanos, TimeUnit.NANOSECONDS),
          FileUtils.byteCountToDisplaySize(FileUtils.sizeOfDirectory(dataDirectory.toFile())));
    } catch (Exception e) {
      LOG.error("Error handling chunk assignment for cache slot {}:", cacheSlotMetadata, e);
      assignmentTimer.stop(chunkAssignmentTimerFailure);
      // If any error occurs during the chunk assignment, evict the chunk so eviction code cleans up
      // the files.
      setChunkMetadataState(cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.EVICT);
    } finally {
      chunkAssignmentLock.unlock();
    }
  }

  private SearchMetadata registerSearchMetadata(
      SearchMetadataStore searchMetadataStore,
      SearchContext cacheSearchContext,
      String snapshotName) {
    CacheNodeMetadata cacheNodeMetadata =
        this.cacheNodeMetadataStore.getSync(getCacheNodeAssignment().cacheNodeId);
    SearchMetadata metadata =
        new SearchMetadata(
            SearchMetadata.generateSearchContextSnapshotId(
                snapshotName, cacheSearchContext.hostname),
            snapshotName,
            cacheSearchContext.toUrl(),
            cacheNodeMetadata.searchable);
    searchMetadataStore.createSync(metadata);
    return metadata;
  }

  // We lock access when manipulating the chunk, as the close()
  // can run concurrently with an eviction
  private void handleChunkEviction(CacheSlotMetadata cacheSlotMetadata) {
    Timer.Sample evictionTimer = Timer.start(meterRegistry);
    chunkAssignmentLock.lock();
    try {
      if (!setChunkMetadataState(
          cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.EVICTING)) {
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
          cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.FREE)) {
        throw new InterruptedException("Failed to set chunk metadata state to free");
      }

      evictionTimer.stop(chunkEvictionTimerSuccess);
    } catch (Exception e) {
      // leave the slot state stuck in evicting, as something is broken, and we don't want a
      // re-assignment or queries hitting this slot
      LOG.error("Error handling chunk eviction", e);
      evictionTimer.stop(chunkEvictionTimerFailure);
    } finally {
      chunkAssignmentLock.unlock();
    }
  }

  private boolean setChunkMetadataState(
      CacheSlotMetadata cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState newState) {
    try {
      cacheSlotMetadataStore
          .updateNonFreeCacheSlotState(cacheSlotMetadata, newState)
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
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
        LOG.error("Error removing files {}", dataDirectory.toString(), e);
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
    cacheNodeMetadataStore.close();
    if (Boolean.getBoolean(ASTRA_NG_DYNAMIC_CHUNK_SIZES_FLAG)) {
      evictChunk(getCacheNodeAssignment());
      cacheNodeAssignmentStore.close();
      replicaMetadataStore.close();
      snapshotMetadataStore.close();
      searchMetadataStore.close();

      LOG.debug("Closed chunk");
    } else {
      CacheSlotMetadata cacheSlotMetadata =
          cacheSlotMetadataStore.getSync(searchContext.hostname, slotId);
      if (cacheSlotMetadata.cacheSlotState != Metadata.CacheSlotMetadata.CacheSlotState.FREE) {
        // Attempt to evict the chunk
        handleChunkEviction(cacheSlotMetadata);
      }
      cacheSlotMetadataStore.removeListener(cacheSlotListener);
      cacheSlotMetadataStore.close();
      LOG.debug("Closed chunk");
    }
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
          query.howMany,
          query.queryBuilder,
          query.sourceFieldFilter,
          query.aggregatorFactoriesBuilder);
    } else {
      return (SearchResult<T>) SearchResult.empty();
    }
  }
}
