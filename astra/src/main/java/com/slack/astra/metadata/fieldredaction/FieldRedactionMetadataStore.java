package com.slack.astra.metadata.fieldredaction;

import com.slack.astra.metadata.core.AstraMetadataStore;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class FieldRedactionMetadataStore extends AstraMetadataStore<FieldRedactionMetadata> {
  public static final String REDACTED_FIELD_METADATA_STORE_ZK_PATH = "/redaction";

  public FieldRedactionMetadataStore(AsyncCuratorFramework curatorFramework, boolean shouldCache) {
    super(
        curatorFramework,
        CreateMode.PERSISTENT,
        shouldCache,
        new FieldRedactionMetadataSerializer().toModelSerializer(),
        REDACTED_FIELD_METADATA_STORE_ZK_PATH);
  }
}
