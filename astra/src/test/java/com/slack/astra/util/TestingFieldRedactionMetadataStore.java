package com.slack.astra.util;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;

public class TestingFieldRedactionMetadataStore extends FieldRedactionMetadataStore {

  //    public FieldRedactionMetadataStore(AsyncCuratorFramework curatorFramework, boolean
  // shouldCache) {
  public TestingFieldRedactionMetadataStore() {
    super(null, false);
  }
}
