package com.slack.kaldb.metadata.service;

import com.slack.kaldb.metadata.core.PersistentMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceMetadataStore extends PersistentMutableMetadataStore<ServiceMetadata> {
  public static final String SERVICE_METADATA_STORE_ZK_PATH = "/service";

  private static final Logger LOG = LoggerFactory.getLogger(ServiceMetadataStore.class);

  public ServiceMetadataStore(MetadataStore metadataStore, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        false,
        SERVICE_METADATA_STORE_ZK_PATH,
        metadataStore,
        new ServiceMetadataSerializer(),
        LOG);
  }
}
