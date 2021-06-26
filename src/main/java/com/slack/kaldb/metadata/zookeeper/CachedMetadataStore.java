package com.slack.kaldb.metadata.zookeeper;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

public interface CachedMetadataStore<T> extends PathChildrenCacheListener {
  List<T> getInstances();

  Optional<T> get(String path);

  void start() throws Exception;

  void close();

  void addListener(CachedMetadataStoreListener listener);

  void addListener(CachedMetadataStoreListener listener, Executor executor);

  void removeListener(CachedMetadataStoreListener listener);
}
