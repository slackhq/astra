package com.slack.astra.util;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import java.util.List;
import org.apache.curator.x.async.AsyncCuratorFramework;

public class TestingFieldRedactionMetadataStore extends FieldRedactionMetadataStore {

  public TestingFieldRedactionMetadataStore(
      AsyncCuratorFramework curatorFramework, boolean shouldCache) {
    super(curatorFramework, shouldCache);
  }

  @Override
  public void createSync(FieldRedactionMetadata metadataNode) {}

  @Override
  public List<FieldRedactionMetadata> listSync() {
    return List.of();
  }
}
