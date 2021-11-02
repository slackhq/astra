package com.slack.kaldb.metadata.cache;

import com.slack.kaldb.metadata.core.EphemeralMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheSlotMetadataStore extends EphemeralMutableMetadataStore<CacheSlotMetadata> {
  private static final Logger LOG = LoggerFactory.getLogger(CacheSlotMetadataStore.class);
  public static final String CACHE_SLOT_ZK_PATH = "/cacheSlot";

  public CacheSlotMetadataStore(MetadataStore metadataStore, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        true,
        CACHE_SLOT_ZK_PATH,
        metadataStore,
        new CacheSlotMetadataSerializer(),
        LOG);
  }

  public CacheSlotMetadataStore(
      MetadataStore metadataStore, String cacheSlotName, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        false,
        String.format("%s/%s", CACHE_SLOT_ZK_PATH, cacheSlotName),
        metadataStore,
        new CacheSlotMetadataSerializer(),
        LOG);
  }
}
