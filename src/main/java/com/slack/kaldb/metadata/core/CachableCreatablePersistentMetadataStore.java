package com.slack.kaldb.metadata.core;

import com.slack.kaldb.metadata.zookeeper.CachedMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import java.util.Optional;
import org.slf4j.Logger;

public class CachableCreatablePersistentMetadataStore<T extends KaldbMetadata>
    extends CreatablePersistentMetadataStore<T> {

  private final Optional<CachedMetadataStore<T>> cache;

  public CachableCreatablePersistentMetadataStore(
      boolean shouldCache,
      String snapshotStoreFolder,
      MetadataStore metadataStore,
      MetadataSerializer<T> metadataSerializer,
      Logger logger)
      throws Exception {

    super(metadataStore, snapshotStoreFolder, metadataSerializer, logger);

    // TODO: Log the no node exception when we fail to create the cache.
    // TODO: Log the cache creation.
    // TODO: Add a listener.
    cache =
        shouldCache
            ? Optional.of(
                metadataStore.cacheNodeAndChildren(snapshotStoreFolder, null, metadataSerializer))
            : Optional.empty();
  }
}
