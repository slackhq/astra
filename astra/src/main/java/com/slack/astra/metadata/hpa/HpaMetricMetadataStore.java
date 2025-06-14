package com.slack.astra.metadata.hpa;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.EtcdCreateMode;
import com.slack.astra.metadata.core.EtcdMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class HpaMetricMetadataStore extends AstraMetadataStore<HpaMetricMetadata> {
  public static final String AUTOSCALER_METADATA_STORE_PATH = "/hpa_metrics";

  public HpaMetricMetadataStore(
      AsyncCuratorFramework curator,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    super(
        curator != null
            ? new ZookeeperMetadataStore<>(
                curator,
                metadataStoreConfig.getZookeeperConfig(),
                CreateMode.EPHEMERAL,
                shouldCache,
                new HpaMetricMetadataSerializer().toModelSerializer(),
                AUTOSCALER_METADATA_STORE_PATH,
                meterRegistry)
            : null,
        etcdClient != null
            ? new EtcdMetadataStore<>(
                AUTOSCALER_METADATA_STORE_PATH,
                metadataStoreConfig.getEtcdConfig(),
                shouldCache,
                meterRegistry,
                new HpaMetricMetadataSerializer(),
                EtcdCreateMode.EPHEMERAL,
                etcdClient)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "HpaMetricMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);
  }
}
