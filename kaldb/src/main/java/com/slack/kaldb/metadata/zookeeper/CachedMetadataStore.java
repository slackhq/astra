package com.slack.kaldb.metadata.zookeeper;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

// TODO: Prefix this class with ZK to clarify this is a ZK cache.
public interface CachedMetadataStore<T> {
  List<T> getInstances();

  Optional<T> get(String relativePathName);

  void start() throws Exception;

  void close();

  void addListener(CachedMetadataStoreListener listener);

  void addListener(CachedMetadataStoreListener listener, Executor executor);

  void removeListener(CachedMetadataStoreListener listener);
}
