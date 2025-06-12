package com.slack.astra.metadata.fieldredaction;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class FieldRedactionMetadataStore extends AstraMetadataStore<FieldRedactionMetadata> {
  public static final String REDACTED_FIELD_METADATA_STORE_ZK_PATH = "/redaction";

  public FieldRedactionMetadataStore(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    super(
        new ZookeeperMetadataStore<>(
            curatorFramework,
            metadataStoreConfig.getZookeeperConfig(),
            CreateMode.PERSISTENT,
            shouldCache,
            new FieldRedactionMetadataSerializer().toModelSerializer(),
            REDACTED_FIELD_METADATA_STORE_ZK_PATH,
            meterRegistry),
        null, // Not using etcdStore for now
        metadataStoreConfig.getMode(),
        meterRegistry);
  }
}
