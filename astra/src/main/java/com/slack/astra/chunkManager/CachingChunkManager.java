package com.slack.astra.chunkManager;

import static com.slack.astra.clusterManager.CacheNodeAssignmentService.snapshotMetadataBySnapshotId;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.chunk.Chunk;
import com.slack.astra.chunk.ReadOnlyChunkImpl;
import com.slack.astra.chunk.SearchContext;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chunk manager implementation that supports loading chunks from S3. All chunks are readonly, and
 * commands to operate with the chunks are made available through ZK.
 */
public class CachingChunkManager<T> extends ChunkManagerBase<T> {
  private static final Logger LOG = LoggerFactory.getLogger(CachingChunkManager.class);
  public static final String ASTRA_NG_DYNAMIC_CHUNK_SIZES_FLAG = "astra.ng.dynamicChunkSizes";

  private final MeterRegistry meterRegistry;
  private final AsyncCuratorFramework curatorFramework;
  private final AstraConfigs.MetadataStoreConfig metadataStoreConfig;
  private final BlobStore blobStore;
  private final SearchContext searchContext;
  private final String s3Bucket;
  private final String dataDirectoryPrefix;
  private final String replicaSet;
  private final int slotCountPerInstance;
  private final AstraMetadataStoreChangeListener<CacheNodeAssignment>
      cacheNodeAssignmentChangeListener = this::onAssignmentHandler;
  private final long capacityBytes;
  private ReplicaMetadataStore replicaMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private SearchMetadataStore searchMetadataStore;
  private CacheSlotMetadataStore cacheSlotMetadataStore;

  // for flag "astra.ng.dynamicChunkSizes"
  private final String cacheNodeId;
  private CacheNodeAssignmentStore cacheNodeAssignmentStore;
  private CacheNodeMetadataStore cacheNodeMetadataStore;

  private final ExecutorService executorService =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder().setNameFormat("caching-chunk-manager-%d").build());

  public CachingChunkManager(
      MeterRegistry registry,
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      BlobStore blobStore,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      String replicaSet,
      int slotCountPerInstance,
      long capacityBytes) {
    this.meterRegistry = registry;
    this.curatorFramework = curatorFramework;
    this.metadataStoreConfig = metadataStoreConfig;
    this.blobStore = blobStore;
    this.searchContext = searchContext;
    this.s3Bucket = s3Bucket;
    this.dataDirectoryPrefix = dataDirectoryPrefix;
    this.replicaSet = replicaSet;
    this.slotCountPerInstance = slotCountPerInstance;
    this.cacheNodeId = UUID.randomUUID().toString();
    this.capacityBytes = capacityBytes;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting caching chunk manager");

    replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    searchMetadataStore =
        new SearchMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, false);
    cacheSlotMetadataStore =
        new CacheSlotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    cacheNodeAssignmentStore =
        new CacheNodeAssignmentStore(
            curatorFramework, metadataStoreConfig, meterRegistry, cacheNodeId);
    cacheNodeMetadataStore =
        new CacheNodeMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);

    if (Boolean.getBoolean(ASTRA_NG_DYNAMIC_CHUNK_SIZES_FLAG)) {
      cacheNodeAssignmentStore.addListener(cacheNodeAssignmentChangeListener);
      cacheNodeMetadataStore.createSync(
          new CacheNodeMetadata(
              cacheNodeId, searchContext.hostname, capacityBytes, replicaSet, false));
      LOG.info(
          "New cache node registered with {} bytes capacity and ID {}", capacityBytes, cacheNodeId);
    } else {
      for (int i = 0; i < slotCountPerInstance; i++) {
        ReadOnlyChunkImpl<T> newChunk =
            new ReadOnlyChunkImpl<>(
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

        chunkMap.put(newChunk.getSlotId(), newChunk);
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing caching chunk manager.");

    chunkMap
        .values()
        .forEach(
            chunk -> {
              try {
                chunk.close();
              } catch (IOException e) {
                LOG.error("Error closing readonly chunk", e);
              }
            });

    if (Boolean.getBoolean(ASTRA_NG_DYNAMIC_CHUNK_SIZES_FLAG)) {
      cacheNodeAssignmentStore.removeListener(cacheNodeAssignmentChangeListener);
      cacheNodeMetadataStore.deleteSync(cacheNodeId);
    }

    cacheNodeMetadataStore.close();
    cacheNodeAssignmentStore.close();
    cacheSlotMetadataStore.close();
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    replicaMetadataStore.close();

    LOG.info("Closed caching chunk manager.");
  }

  public static CachingChunkManager<LogMessage> fromConfig(
      MeterRegistry meterRegistry,
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      AstraConfigs.S3Config s3Config,
      AstraConfigs.CacheConfig cacheConfig,
      BlobStore blobStore)
      throws Exception {
    return new CachingChunkManager<>(
        meterRegistry,
        curatorFramework,
        metadataStoreConfig,
        blobStore,
        SearchContext.fromConfig(cacheConfig.getServerConfig()),
        s3Config.getS3Bucket(),
        cacheConfig.getDataDirectory(),
        cacheConfig.getReplicaSet(),
        cacheConfig.getSlotsPerInstance(),
        cacheConfig.getCapacityBytes());
  }

  @Override
  public void addMessage(Trace.Span message, long msgSize, String kafkaPartitionId, long offset)
      throws IOException {
    throw new UnsupportedOperationException(
        "Adding messages is not supported on a caching chunk manager");
  }

  private void onAssignmentHandler(CacheNodeAssignment assignment) {
    if (Objects.equals(assignment.cacheNodeId, this.cacheNodeId)) {
      LOG.info(
          "Assignment handler fired for cache node {} and assignment {}",
          cacheNodeId,
          assignment.assignmentId);
      Map<String, SnapshotMetadata> snapshotsBySnapshotId =
          snapshotMetadataBySnapshotId(snapshotMetadataStore);
      try {
        if (chunkMap.containsKey(assignment.assignmentId)) {
          ReadOnlyChunkImpl<T> chunk = (ReadOnlyChunkImpl) chunkMap.get(assignment.assignmentId);

          if (chunkStateChangedToEvict(assignment, chunk)) {
            LOG.info(
                "Starting eviction for assignment {} from node {}",
                assignment.assignmentId,
                cacheNodeId);
            chunk.evictChunk(assignment);
            chunkMap.remove(assignment.assignmentId);
            LOG.info("Evicted assignment {} from node {}", assignment.assignmentId, cacheNodeId);
          } else if (assignment.state == chunk.getLastKnownAssignmentState()) {
            LOG.info("Chunk listener fired, but state remained the same");
          }
        } else {
          if (assignment.state != Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING) {
            LOG.info(
                "Encountered an new assignment with a non LOADING state, state: {}",
                assignment.state);
          } else {
            LOG.info(
                "Created new chunk for assignment {} in cache node {}",
                assignment.assignmentId,
                cacheNodeId);
            ReadOnlyChunkImpl<T> newChunk =
                new ReadOnlyChunkImpl<>(
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
                    cacheNodeAssignmentStore,
                    assignment,
                    snapshotsBySnapshotId.get(assignment.snapshotId),
                    cacheNodeMetadataStore);
            var unused = executorService.submit(newChunk::downloadChunkData);
            chunkMap.put(assignment.assignmentId, newChunk);
          }
        }
      } catch (Exception e) {
        LOG.error("Error instantiating readonly chunk", e);
      }
    }
  }

  private static <T> boolean chunkStateChangedToEvict(
      CacheNodeAssignment assignment, ReadOnlyChunkImpl<T> chunk) {
    return (chunk.getLastKnownAssignmentState() != assignment.state)
        && (assignment.state == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT);
  }

  public String getId() {
    return cacheNodeId;
  }

  public Map<String, Chunk<T>> getChunksMap() {
    return chunkMap;
  }
}
