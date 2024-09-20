package com.slack.astra.metadata.redactedfield;

import com.slack.astra.metadata.core.AstraMetadataStore;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class RedactedFieldMetadataStore extends AstraMetadataStore<RedactedFieldMetadata> {
  public static final String REDACTED_FIELD_METADATA_STORE_ZK_PATH = "/redaction";

  public RedactedFieldMetadataStore(AsyncCuratorFramework curatorFramework, boolean shouldCache) {
    super(
        curatorFramework,
        CreateMode.PERSISTENT,
        shouldCache,
        new RedactedFieldMetadataSerializer().toModelSerializer(),
        REDACTED_FIELD_METADATA_STORE_ZK_PATH);
  }
}
