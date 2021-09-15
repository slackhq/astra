package com.slack.kaldb.server;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;

public class MetadataStoreService extends AbstractIdleService {
  private final MeterRegistry meterRegistry;
  private final KaldbConfigs.ZookeeperConfig zookeeperConfig;
  private MetadataStore metadataStore;

  public MetadataStoreService(MeterRegistry meterRegistry) {
    this(meterRegistry, KaldbConfig.get().getMetadataStoreConfig().getZookeeperConfig());
  }

  public MetadataStoreService(
      MeterRegistry meterRegistry, KaldbConfigs.ZookeeperConfig zookeeperConfig) {
    this.meterRegistry = meterRegistry;
    this.zookeeperConfig = zookeeperConfig;
  }

  @Override
  protected void startUp() {
    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zookeeperConfig);
  }

  @Override
  protected void shutDown() throws Exception {
    metadataStore.close();
  }

  public MetadataStore getMetadataStore() {
    return metadataStore;
  }
}
