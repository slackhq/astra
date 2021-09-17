package com.slack.kaldb.chunk.manager.caching;

import com.slack.kaldb.chunk.manager.ChunkManager;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachingChunkManager<T> extends ChunkManager<T> {
  private static final Logger LOG = LoggerFactory.getLogger(CachingChunkManager.class);

  private final MeterRegistry meterRegistry;
  private final MetadataStoreService metadataStoreService;

  public CachingChunkManager(MeterRegistry registry, MetadataStoreService metadataStoreService) {
    this.meterRegistry = registry;
    this.metadataStoreService = metadataStoreService;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting caching chunk manager");
    metadataStoreService.awaitRunning(KaldbConfig.DEFAULT_START_STOP_DURATION);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing caching chunk manager.");
    LOG.info("Closed caching chunk manager.");
  }

  public static CachingChunkManager<LogMessage> fromConfig(
      MeterRegistry meterRegistry,
      MetadataStoreService metadataStoreService,
      KaldbConfigs.ServerConfig serverConfig) {

    // TODO: Read the config values for chunk manager from config file.
    CachingChunkManager<LogMessage> chunkManager =
        new CachingChunkManager<>(meterRegistry, metadataStoreService);

    return chunkManager;
  }
}
