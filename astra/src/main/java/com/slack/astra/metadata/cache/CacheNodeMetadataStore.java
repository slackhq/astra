package com.slack.astra.metadata.cache;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class CacheNodeMetadataStore extends AstraMetadataStore<CacheNodeMetadata> {
  public static final String CACHE_NODE_METADATA_STORE_ZK_PATH = "/cacheNodes";

  public CacheNodeMetadataStore(
      AsyncCuratorFramework curator,
      AstraConfigs.ZookeeperConfig zkConfig,
      MeterRegistry meterRegistry) {
    super(
        curator,
        zkConfig,
        CreateMode.EPHEMERAL,
        true,
        new CacheNodeMetadataSerializer().toModelSerializer(),
        CACHE_NODE_METADATA_STORE_ZK_PATH,
        meterRegistry);
  }
}
