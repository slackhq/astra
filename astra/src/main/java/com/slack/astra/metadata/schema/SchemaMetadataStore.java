package com.slack.astra.metadata.schema;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.EtcdCreateMode;
import com.slack.astra.metadata.core.EtcdMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class SchemaMetadataStore extends AstraMetadataStore<SchemaMetadata> {
  public static final String SCHEMA_METADATA_STORE_PATH = "/schema";

  public SchemaMetadataStore(
      AsyncCuratorFramework curatorFramework,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    super(
        curatorFramework != null
            ? new ZookeeperMetadataStore<>(
                curatorFramework,
                metadataStoreConfig.getZookeeperConfig(),
                CreateMode.PERSISTENT,
                shouldCache,
                new SchemaMetadataSerializer().toModelSerializer(),
                SCHEMA_METADATA_STORE_PATH,
                meterRegistry)
            : null,
        etcdClient != null
            ? new EtcdMetadataStore<>(
                SCHEMA_METADATA_STORE_PATH,
                metadataStoreConfig.getEtcdConfig(),
                shouldCache,
                meterRegistry,
                new SchemaMetadataSerializer(),
                EtcdCreateMode.PERSISTENT,
                etcdClient)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "SchemaMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);
  }
}
