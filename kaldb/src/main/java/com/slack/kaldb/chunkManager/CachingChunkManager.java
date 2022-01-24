package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.blobfs.s3.S3BlobFs.getS3BlobFsClient;

import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.ReadOnlyChunkImpl;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.config.KaldbConfig;
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
public class CachingChunkManager<T> extends ChunkManager<T> {
  private static final Logger LOG = LoggerFactory.getLogger(CachingChunkManager.class);

  private final MeterRegistry meterRegistry;
  private final MetadataStore metadataStore;
  private final S3BlobFs s3BlobFs;
  private final SearchContext searchContext;
  private final String s3Bucket;
  private final String dataDirectoryPrefix;
  private final int slotCountPerInstance;

  public CachingChunkManager(
      MeterRegistry registry,
      MetadataStore metadataStore,
      S3BlobFs s3BlobFs,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      int slotCountPerInstance) {
    this.meterRegistry = registry;
    this.metadataStore = metadataStore;
    this.s3BlobFs = s3BlobFs;
    this.searchContext = searchContext;
    this.s3Bucket = s3Bucket;
    this.dataDirectoryPrefix = dataDirectoryPrefix;
    this.slotCountPerInstance = slotCountPerInstance;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting caching chunk manager");

    ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(metadataStore, false);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(metadataStore, false);
    CacheSlotMetadataStore cacheSlotMetadataStore =
        new CacheSlotMetadataStore(metadataStore, false);

    for (int i = 0; i < slotCountPerInstance; i++) {
      chunkList.add(
          new ReadOnlyChunkImpl<>(
              metadataStore,
              meterRegistry,
              s3BlobFs,
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

    LOG.info("Closed caching chunk manager.");
  }

  public static CachingChunkManager<LogMessage> fromConfig(
      MeterRegistry meterRegistry,
      MetadataStore metadataStore,
      KaldbConfigs.ServerConfig serverConfig)
      throws Exception {
    return new CachingChunkManager<>(
        meterRegistry,
        metadataStore,
        getS3BlobFsClient(KaldbConfig.get().getS3Config()),
        SearchContext.fromConfig(serverConfig),
        KaldbConfig.get().getS3Config().getS3Bucket(),
        KaldbConfig.get().getCacheConfig().getDataDirectory(),
        KaldbConfig.get().getCacheConfig().getSlotsPerInstance());
  }
}
