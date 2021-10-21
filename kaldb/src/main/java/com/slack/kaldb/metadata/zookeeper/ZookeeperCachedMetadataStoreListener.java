package com.slack.kaldb.metadata.zookeeper;

import org.apache.curator.framework.state.ConnectionStateListener;

/** Listener for listening to changes in CachedMetadataStore */
public interface ZookeeperCachedMetadataStoreListener extends ConnectionStateListener {
  /**
   * Called when cache is changed (instances added/deleted or updated).
   *
   * <p>All callbacks are executed in a single thread executor, prefer delegating long-running tasks
   * to a separate threadpool.
   */
  void cacheChanged();
}
