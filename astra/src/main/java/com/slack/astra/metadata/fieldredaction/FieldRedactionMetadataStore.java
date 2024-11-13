package com.slack.astra.metadata.fieldredaction;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class FieldRedactionMetadataStore extends AstraMetadataStore<FieldRedactionMetadata> {
  public static final String REDACTED_FIELD_METADATA_STORE_ZK_PATH = "/redaction";

  public FieldRedactionMetadataStore(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.ZookeeperConfig zkConfig,
      boolean shouldCache) {
    super(
        curatorFramework,
        zkConfig,
        CreateMode.PERSISTENT,
        shouldCache,
        new FieldRedactionMetadataSerializer().toModelSerializer(),
        REDACTED_FIELD_METADATA_STORE_ZK_PATH);
  }
}
