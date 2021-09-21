package com.slack.kaldb.chunk;

import static com.slack.kaldb.config.KaldbConfig.CACHE_SLOT_STORE_ZK_PATH;
import static com.slack.kaldb.metadata.cache.CacheSlotMetadata.METADATA_SLOT_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.logstore.search.LogIndexSearcher;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.metadata.cache.CacheSlotMetadata;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.server.MetadataStoreService;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
  private final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final ExecutorService executorService;

  public ReadOnlyChunkImpl(
      String chunkId,
      MetadataStoreService metadataStoreService,
      KaldbConfigs.CacheConfig cacheConfig)
      throws Exception {
    this.chunkId = chunkId;
    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(String.format("readonly-chunk-%s-%%d", chunkId))
                .build());

    String serverAddress = cacheConfig.getServerConfig().getServerAddress();
    String slotId = String.format("%s-%s", serverAddress, chunkId);

    String cacheSlotPath = String.format("%s/%s", CACHE_SLOT_STORE_ZK_PATH, slotId);
    cacheSlotMetadataStore =
        new CacheSlotMetadataStore(metadataStoreService.getMetadataStore(), cacheSlotPath, true);
    cacheSlotMetadataStore.addListener(cacheNodeListener());

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
                handleChunkAssignment();
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

  private void handleChunkAssignment() throws Exception {
    setChunkMetadataState(Metadata.CacheSlotState.LOADING);

    // todo - load asset from S3 into chunkManager

    // // todo - move to CacheConfig?
    //    Path dataDirectory = Path.of(KaldbConfig.get().getIndexerConfig().getDataDirectory());

    //    this.logSearcher =
    //        (LogIndexSearcher<T>)
    //            new
    // LogIndexSearcherImpl(LogIndexSearcherImpl.searcherManagerFromPath(dataDirectory));
    // this.chunkinfo = ?

    setChunkMetadataState(Metadata.CacheSlotState.LIVE);
  }

  private void handleChunkEviction() throws Exception {
    setChunkMetadataState(Metadata.CacheSlotState.EVICTING);

    chunkInfo = null;
    logSearcher = null;

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
