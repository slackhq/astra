package com.slack.astra.chunkManager;

import com.slack.astra.blobfs.BlobFs;
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
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chunk manager implementation that supports loading chunks from S3. All chunks are readonly, and
 * commands to operate with the chunks are made available through ZK.
 */
public class CachingChunkManager<T> extends ChunkManagerBase<T> {
  private static final Logger LOG = LoggerFactory.getLogger(CachingChunkManager.class);

  private final MeterRegistry meterRegistry;
  private final AsyncCuratorFramework curatorFramework;
  private final BlobFs blobFs;
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
  private final Map<String, ReadOnlyChunkImpl<T>> chunksMap;

  public CachingChunkManager(
      MeterRegistry registry,
      AsyncCuratorFramework curatorFramework,
      BlobFs blobFs,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      String replicaSet,
      int slotCountPerInstance,
      long capacityBytes) {
    this.meterRegistry = registry;
    this.curatorFramework = curatorFramework;
    this.blobFs = blobFs;
    this.searchContext = searchContext;
    this.s3Bucket = s3Bucket;
    this.dataDirectoryPrefix = dataDirectoryPrefix;
    this.replicaSet = replicaSet;
    this.slotCountPerInstance = slotCountPerInstance;
    this.cacheNodeId = UUID.randomUUID().toString();
    this.capacityBytes = capacityBytes;
    this.chunksMap = new HashMap<>();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting caching chunk manager");

    replicaMetadataStore = new ReplicaMetadataStore(curatorFramework);
    snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    searchMetadataStore = new SearchMetadataStore(curatorFramework, false);
    cacheSlotMetadataStore = new CacheSlotMetadataStore(curatorFramework);
    cacheNodeAssignmentStore = new CacheNodeAssignmentStore(curatorFramework);
    cacheNodeMetadataStore = new CacheNodeMetadataStore(curatorFramework);

    if (Boolean.getBoolean("astra.ng.dynamicChunkSizes")) {
      cacheNodeAssignmentStore.addListener(cacheNodeAssignmentChangeListener);
      cacheNodeMetadataStore.createSync(
          new CacheNodeMetadata(cacheNodeId, searchContext.hostname, capacityBytes, replicaSet));
    } else {
      for (int i = 0; i < slotCountPerInstance; i++) {
        chunkList.add(
            new ReadOnlyChunkImpl<>(
                curatorFramework,
                meterRegistry,
                blobFs,
                searchContext,
                s3Bucket,
                dataDirectoryPrefix,
                replicaSet,
                cacheSlotMetadataStore,
                replicaMetadataStore,
                snapshotMetadataStore,
                searchMetadataStore));
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing caching chunk manager.");

    if (Boolean.getBoolean("astra.ng.dynamicChunkSizes")) {
      chunksMap
          .values()
          .forEach(
              chunk -> {
                try {
                  chunk.close();
                } catch (IOException e) {
                  LOG.error("Error closing readonly chunk", e);
                }
              });
      cacheNodeAssignmentStore.removeListener(cacheNodeAssignmentChangeListener);
      cacheNodeMetadataStore.deleteSync(cacheNodeId);
    } else {
      chunkList.forEach(
          (readonlyChunk) -> {
            try {
              readonlyChunk.close();
            } catch (IOException e) {
              LOG.error("Error closing readonly chunk", e);
            }
          });
    }

    cacheSlotMetadataStore.close();
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    replicaMetadataStore.close();
    cacheNodeAssignmentStore.close();

    LOG.info("Closed caching chunk manager.");
  }

  public static CachingChunkManager<LogMessage> fromConfig(
      MeterRegistry meterRegistry,
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.S3Config s3Config,
      AstraConfigs.CacheConfig cacheConfig,
      BlobFs blobFs)
      throws Exception {
    return new CachingChunkManager<>(
        meterRegistry,
        curatorFramework,
        blobFs,
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
      try {
        if (chunksMap.containsKey(assignment.assignmentId)) {
          ReadOnlyChunkImpl<T> chunk = chunksMap.get(assignment.assignmentId);

          if (chunkStateChangedToEvict(assignment, chunk)) {
            chunk.evictChunk(assignment);
            chunksMap.remove(assignment.assignmentId);
          }
        } else {
          ReadOnlyChunkImpl<T> newChunk =
              new ReadOnlyChunkImpl<>(
                  curatorFramework,
                  meterRegistry,
                  blobFs,
                  searchContext,
                  s3Bucket,
                  dataDirectoryPrefix,
                  replicaSet,
                  cacheSlotMetadataStore,
                  replicaMetadataStore,
                  snapshotMetadataStore,
                  searchMetadataStore,
                  cacheNodeAssignmentStore,
                  assignment.assignmentId,
                  assignment.cacheNodeId);
          chunksMap.put(assignment.assignmentId, newChunk);
        }
      } catch (Exception e) {
        LOG.error("Error instantiating readonly chunk", e);
      }
    }
  }

  private static <T> boolean chunkStateChangedToEvict(
      CacheNodeAssignment assignment, ReadOnlyChunkImpl<T> chunk) {
    return (chunk.getLastKnownAssignmentState() != assignment.state)
        && (chunk.getLastKnownAssignmentState()
            == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE)
        && (assignment.state == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT);
  }

  public String getId() {
    return cacheNodeId;
  }

  public Map<String, ReadOnlyChunkImpl<T>> getChunksMap() {
    return chunksMap;
  }
}
