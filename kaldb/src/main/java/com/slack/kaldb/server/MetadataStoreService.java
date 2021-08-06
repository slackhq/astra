package com.slack.kaldb.server;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import io.micrometer.core.instrument.MeterRegistry;

public class MetadataStoreService extends AbstractIdleService {
  private MetadataStore metadataStore;

  MetadataStoreService(MeterRegistry meterRegistry) {
    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry);
  }

  @Override
  protected void startUp() throws Exception {}

  @Override
  protected void shutDown() throws Exception {
    metadataStore.close();
  }
}
