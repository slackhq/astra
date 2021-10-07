package com.slack.kaldb.chunk.manager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;

import com.slack.kaldb.chunk.ReadOnlyChunkImpl;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.UUID;
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
  private final KaldbConfigs.CacheConfig cacheConfig;

  public CachingChunkManager(
      MeterRegistry registry,
      MetadataStoreService metadataStoreService,
      KaldbConfigs.CacheConfig cacheConfig) {
    this.meterRegistry = registry;
    this.metadataStoreService = metadataStoreService;
    this.cacheConfig = cacheConfig;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting caching chunk manager");
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    for (int i = 0; i < cacheConfig.getSlotsPerInstance(); i++) {
      String chunkId = UUID.randomUUID().toString();
      chunkList.add(new ReadOnlyChunkImpl<>(chunkId, metadataStoreService, cacheConfig));
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
      KaldbConfigs.CacheConfig cacheConfig) {
    return new CachingChunkManager<>(meterRegistry, metadataStoreService, cacheConfig);
  }
}
