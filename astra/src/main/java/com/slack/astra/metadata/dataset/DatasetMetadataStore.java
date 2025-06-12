package com.slack.astra.metadata.dataset;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class DatasetMetadataStore extends AstraMetadataStore<DatasetMetadata> {
  // TODO: The path should be dataset, but leaving it as /service for backwards compatibility.
  public static final String DATASET_METADATA_STORE_ZK_PATH = "/service";

  public DatasetMetadataStore(
      AsyncCuratorFramework curator,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    super(
        new ZookeeperMetadataStore<>(
            curator,
            metadataStoreConfig.getZookeeperConfig(),
            CreateMode.PERSISTENT,
            shouldCache,
            new DatasetMetadataSerializer().toModelSerializer(),
            DATASET_METADATA_STORE_ZK_PATH,
            meterRegistry),
        null, // Not using etcdStore for now
        metadataStoreConfig.getMode(),
        meterRegistry);
  }
}
