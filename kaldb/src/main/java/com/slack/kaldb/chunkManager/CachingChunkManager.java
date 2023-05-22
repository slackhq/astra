package com.slack.kaldb.chunkManager;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.chunk.ReadOnlyChunkImpl;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
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

  private final MeterRegistry meterRegistry;
  private final AsyncCuratorFramework curatorFramework;
  private final BlobFs blobFs;
  private final SearchContext searchContext;
  private final String s3Bucket;
  private final String dataDirectoryPrefix;
  private final int slotCountPerInstance;
  private ReplicaMetadataStore replicaMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private SearchMetadataStore searchMetadataStore;
  private CacheSlotMetadataStore cacheSlotMetadataStore;
  private final ExecutorService executorService;

  public CachingChunkManager(
      MeterRegistry registry,
      AsyncCuratorFramework curatorFramework,
      BlobFs blobFs,
      SearchContext searchContext,
      String s3Bucket,
      String dataDirectoryPrefix,
      int slotCountPerInstance) {
    this.meterRegistry = registry;
    this.curatorFramework = curatorFramework;
    this.blobFs = blobFs;
    this.searchContext = searchContext;
    this.s3Bucket = s3Bucket;
    this.dataDirectoryPrefix = dataDirectoryPrefix;
    this.slotCountPerInstance = slotCountPerInstance;

    // todo - consider making the thread count a config option; this would allow for more
    //  fine-grained tuning, but we might not need to expose this to the user if we can set sensible
    //  defaults
    this.executorService =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() >= 4 ? 2 : 1,
            new ThreadFactoryBuilder()
                .setNameFormat("caching-chunk-manager-%d")
                .setUncaughtExceptionHandler(
                    (t, e) -> LOG.error("Exception on thread {}: {}", t.getName(), e))
                .build());
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting caching chunk manager");

    replicaMetadataStore = new ReplicaMetadataStore(curatorFramework, false);
    snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework, false);
    searchMetadataStore = new SearchMetadataStore(curatorFramework, false);
    cacheSlotMetadataStore = new CacheSlotMetadataStore(curatorFramework, false);

    for (int i = 0; i < slotCountPerInstance; i++) {
      chunkList.add(
          new ReadOnlyChunkImpl<>(
              curatorFramework,
              meterRegistry,
              blobFs,
              searchContext,
              s3Bucket,
              dataDirectoryPrefix,
              cacheSlotMetadataStore,
              replicaMetadataStore,
              snapshotMetadataStore,
              searchMetadataStore,
              ProtectedExecutorService.wrap(executorService)));
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing caching chunk manager.");

    // Attempt to forcibly shutdown the executor service. This prevents any further downloading of
    // data from S3 that would be unused.
    executorService.shutdown();

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
      AsyncCuratorFramework curatorFramework,
      KaldbConfigs.S3Config s3Config,
      KaldbConfigs.CacheConfig cacheConfig,
      BlobFs blobFs)
      throws Exception {
    return new CachingChunkManager<>(
        meterRegistry,
        curatorFramework,
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
