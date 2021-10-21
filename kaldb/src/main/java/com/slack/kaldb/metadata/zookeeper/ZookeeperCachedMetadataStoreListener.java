package com.slack.kaldb.metadata.zookeeper;

import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * Listener for listening to changes in ZookeeperCachedMetadataStore.
 *
 * <p>NOTE: Currently, this interface extends ConnectionStateListener, however, this interface is
 * not used until the cache can be refreshed on ZK start/stop events. Leaving it in for now, since
 * it is a lot more work later to add it in.
 */
public interface ZookeeperCachedMetadataStoreListener extends ConnectionStateListener {
  /**
   * Called when cache is changed (instances added/deleted or updated).
   *
   * <p>All callbacks are executed in a single thread executor, prefer delegating long-running tasks
   * to a separate thread pool.
   */
  void cacheChanged();
}
