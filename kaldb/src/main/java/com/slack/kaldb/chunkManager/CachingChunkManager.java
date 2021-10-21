package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.config.KaldbConfig.REPLICA_STORE_ZK_PATH;
import static com.slack.kaldb.config.KaldbConfig.SEARCH_METADATA_STORE_ZK_PATH;
import static com.slack.kaldb.config.KaldbConfig.SNAPSHOT_METADATA_STORE_ZK_PATH;

import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.ReadOnlyChunkImpl;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.MetadataStoreService;
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
  private final MetadataStoreService metadataStoreService;
  private final S3BlobFs s3BlobFs;
  private final SearchContext searchContext;
  private final String s3Bucket;
  private final String dataDirectoryPrefix;
  private final int slotCountPerInstance;

  public CachingChunkManager(
      MeterRegistry registry,
      MetadataStoreService metadataStoreService,
      S3BlobFs s3BlobFs,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      int slotCountPerInstance) {
    this.meterRegistry = registry;
    this.metadataStoreService = metadataStoreService;
    this.s3BlobFs = s3BlobFs;
    this.searchContext = searchContext;
    this.s3Bucket = s3Bucket;
    this.dataDirectoryPrefix = dataDirectoryPrefix;
    this.slotCountPerInstance = slotCountPerInstance;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting caching chunk manager");
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(
            metadataStoreService.getMetadataStore(), REPLICA_STORE_ZK_PATH, false);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(
            metadataStoreService.getMetadataStore(), SNAPSHOT_METADATA_STORE_ZK_PATH, false);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(
            metadataStoreService.getMetadataStore(), SEARCH_METADATA_STORE_ZK_PATH, false);

    for (int i = 0; i < slotCountPerInstance; i++) {
      chunkList.add(
          new ReadOnlyChunkImpl<>(
              metadataStoreService,
              meterRegistry,
              s3BlobFs,
              searchContext,
              s3Bucket,
              dataDirectoryPrefix,
              replicaMetadataStore,
              snapshotMetadataStore,
              searchMetadataStore));
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing caching chunk manager.");

    metadataStoreService.stopAsync();
    metadataStoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);

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
      MetadataStoreService metadataStoreService,
      KaldbConfigs.ServerConfig serverConfig)
      throws Exception {
    return new CachingChunkManager<>(
        meterRegistry,
        metadataStoreService,
        getS3BlobFsClient(KaldbConfig.get()),
        SearchContext.fromConfig(serverConfig),
        KaldbConfig.get().getS3Config().getS3Bucket(),
        KaldbConfig.get().getCacheConfig().getDataDirectory(),
        KaldbConfig.get().getCacheConfig().getSlotsPerInstance());
  }
}
