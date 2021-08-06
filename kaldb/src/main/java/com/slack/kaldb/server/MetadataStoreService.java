package com.slack.kaldb.server;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import io.micrometer.core.instrument.MeterRegistry;

public class MetadataStoreService extends AbstractIdleService {

  public static MetadataStore fromConfig(MeterRegistry meterRegistry) {
    // Zookeeper KaldbConfig.get().getMetadataStoreConfig().getZookeeperConfig();
    return null;
  }

  private MetadataStore metadataStore;

  MetadataStoreService() {}

  @Override
  protected void startUp() throws Exception {}

  @Override
  protected void shutDown() throws Exception {
    metadataStore.close();
  }
}
