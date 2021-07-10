package com.slack.kaldb.metadata.core;

import com.slack.kaldb.metadata.zookeeper.CachedMetadataStore;
import com.slack.kaldb.metadata.zookeeper.CachedMetadataStoreListener;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;

/**
 * This store adds a cache on top of the KaldbMetadataStore. Optionally, this class can disable
 * caching by passing in a feature flag.
 *
 * <p>To use this class, the steps are: instantiate the class, register listeners and start the
 * cache. To close the cache, call the close method.
 */
public abstract class CachableMetadataStore<T extends KaldbMetadata> extends KaldbMetadataStore<T> {

  private final Optional<CachedMetadataStore<T>> cache;

  public CachableMetadataStore(
      boolean shouldCache,
      String snapshotStoreFolder,
      MetadataStore metadataStore,
      MetadataSerializer<T> metadataSerializer,
      Logger logger)
      throws Exception {

    super(metadataStore, snapshotStoreFolder, metadataSerializer, logger);

    if (shouldCache) {
      cache =
          Optional.of(metadataStore.cacheNodeAndChildren(snapshotStoreFolder, metadataSerializer));
      logger.info("Caching nodes for path {}", snapshotStoreFolder);
    } else {
      cache = Optional.empty();
      logger.info("Disabled caching nodes under {}", snapshotStoreFolder);
    }
  }

  public void start() throws Exception {
    if (cache.isPresent()) cache.get().start();
  }

  public void close() {
    cache.ifPresent(CachedMetadataStore::close);
  }

  public List<T> get_cached() {
    return cache.isEmpty() ? Collections.emptyList() : cache.get().getInstances();
  }

  public void addListener(CachedMetadataStoreListener listener) {
    cache.ifPresent(cacheImpl -> cacheImpl.addListener(listener));
  }

  public void removeListener(CachedMetadataStoreListener listener) {
    cache.ifPresent(cacheImpl -> cacheImpl.removeListener(listener));
  }
}
