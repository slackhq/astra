package com.slack.astra.metadata.fieldredaction;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.EtcdCreateMode;
import com.slack.astra.metadata.core.EtcdMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class FieldRedactionMetadataStore extends AstraMetadataStore<FieldRedactionMetadata> {
  public static final String REDACTED_FIELD_METADATA_STORE_PATH = "/redaction";

  public FieldRedactionMetadataStore(
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
                new FieldRedactionMetadataSerializer().toModelSerializer(),
                REDACTED_FIELD_METADATA_STORE_PATH,
                meterRegistry)
            : null,
        etcdClient != null
            ? new EtcdMetadataStore<>(
                REDACTED_FIELD_METADATA_STORE_PATH,
                metadataStoreConfig.getEtcdConfig(),
                shouldCache,
                meterRegistry,
                new FieldRedactionMetadataSerializer(),
                EtcdCreateMode.PERSISTENT,
                etcdClient)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "FieldRedactionMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);
  }
}
