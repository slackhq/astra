package com.slack.kaldb.chunkManager;

import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.chunk.ReadOnlyChunkImpl;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chunk manager implementation that supports loading chunks from S3. All chunks are readonly, and
 * commands to operate with the chunks are made available through ZK.
 */
public class CachingChunkManager<T> extends ChunkManagerBase<T> {
  private static final Logger LOG = LoggerFactory.getLogger(CachingChunkManager.class);

  private final MeterRegistry meterRegistry;
  private final MetadataStore metadataStore;
  private final BlobFs blobFs;
  private final SearchContext searchContext;
  private final String s3Bucket;
  private final String dataDirectoryPrefix;
  private final int slotCountPerInstance;
  private ReplicaMetadataStore replicaMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private SearchMetadataStore searchMetadataStore;
  private CacheSlotMetadataStore cacheSlotMetadataStore;

  public CachingChunkManager(
      MeterRegistry registry,
      MetadataStore metadataStore,
      BlobFs blobFs,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      int slotCountPerInstance) {
    this.meterRegistry = registry;
    this.metadataStore = metadataStore;
    this.blobFs = blobFs;
    this.searchContext = searchContext;
    this.s3Bucket = s3Bucket;
    this.dataDirectoryPrefix = dataDirectoryPrefix;
    this.slotCountPerInstance = slotCountPerInstance;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting caching chunk manager");

    replicaMetadataStore = new ReplicaMetadataStore(metadataStore, false);
    snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    searchMetadataStore = new SearchMetadataStore(metadataStore, false);
    cacheSlotMetadataStore = new CacheSlotMetadataStore(metadataStore, false);

    for (int i = 0; i < slotCountPerInstance; i++) {
      chunkList.add(
          new ReadOnlyChunkImpl<>(
              metadataStore,
              meterRegistry,
              blobFs,
              searchContext,
              s3Bucket,
              dataDirectoryPrefix,
              cacheSlotMetadataStore,
              replicaMetadataStore,
              snapshotMetadataStore,
              searchMetadataStore));
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing caching chunk manager.");

    chunkList.forEach(
        (readonlyChunk) -> {
          try {
            readonlyChunk.close();
          } catch (IOException e) {
            LOG.error("Error closing readonly chunk", e);
          }
        });

    cacheSlotMetadataStore.close();
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    replicaMetadataStore.close();

    LOG.info("Closed caching chunk manager.");
  }

  public static CachingChunkManager<LogMessage> fromConfig(
      MeterRegistry meterRegistry,
      MetadataStore metadataStore,
      KaldbConfigs.S3Config s3Config,
      KaldbConfigs.CacheConfig cacheConfig,
      BlobFs blobFs)
      throws Exception {
    return new CachingChunkManager<>(
        meterRegistry,
        metadataStore,
        blobFs,
        SearchContext.fromConfig(cacheConfig.getServerConfig()),
        s3Config.getS3Bucket(),
        cacheConfig.getDataDirectory(),
        cacheConfig.getSlotsPerInstance());
  }

  @Override
  public void addMessage(T message, long msgSize, String kafkaPartitionId, long offset)
      throws IOException {
    throw new UnsupportedOperationException(
        "Adding messages is not supported on caching chunk manager");
  }
}
