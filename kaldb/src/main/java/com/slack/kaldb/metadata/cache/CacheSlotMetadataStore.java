package com.slack.kaldb.metadata.cache;

import com.slack.kaldb.metadata.core.EphemeralMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheSlotMetadataStore extends EphemeralMutableMetadataStore<CacheSlotMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(CacheSlotMetadataStore.class);

  public CacheSlotMetadataStore(
      MetadataStore metadataStore, String snapshotStorePath, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        true,
        snapshotStorePath,
        metadataStore,
        new CacheSlotMetadataSerializer(),
        LOG);
  }
}
