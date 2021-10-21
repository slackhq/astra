package com.slack.kaldb.metadata.zookeeper;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

public interface ZooKeeperCachedMetadataStore<T> {
  List<T> getInstances();

  Optional<T> get(String relativePathName);

  void start() throws Exception;

  void close();

  void addListener(ZookeeperCachedMetadataStoreListener listener);

  void addListener(ZookeeperCachedMetadataStoreListener listener, Executor executor);

  void removeListener(ZookeeperCachedMetadataStoreListener listener);
}
