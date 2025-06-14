package com.slack.astra.metadata.cache;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.EtcdCreateMode;
import com.slack.astra.metadata.core.EtcdMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class CacheNodeMetadataStore extends AstraMetadataStore<CacheNodeMetadata> {
  public static final String CACHE_NODE_METADATA_STORE_PATH = "/cacheNodes";

  public CacheNodeMetadataStore(
      AsyncCuratorFramework curator,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry) {
    super(
        curator != null
            ? new ZookeeperMetadataStore<>(
                curator,
                metadataStoreConfig.getZookeeperConfig(),
                CreateMode.EPHEMERAL,
                true,
                new CacheNodeMetadataSerializer().toModelSerializer(),
                CACHE_NODE_METADATA_STORE_PATH,
                meterRegistry)
            : null,
        etcdClient != null
            ? new EtcdMetadataStore<>(
                CACHE_NODE_METADATA_STORE_PATH,
                metadataStoreConfig.getEtcdConfig(),
                true,
                meterRegistry,
                new CacheNodeMetadataSerializer(),
                EtcdCreateMode.EPHEMERAL,
                etcdClient)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "CacheNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);
  }
}
