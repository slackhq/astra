package com.slack.astra.metadata.core;

/**
 * CreateMode defines the types of nodes that can be created in etcd. This is similar to ZooKeeper's
 * CreateMode but adapted for etcd capabilities.
 *
 * <p>Etcd doesn't have a direct equivalent to ZooKeeper's ephemeral nodes, so we simulate them
 * using TTL (time-to-live) leases.
 */
public enum EtcdCreateMode {
  /** The node will not be automatically deleted. */
  PERSISTENT,

  /**
   * The node will be deleted automatically when the session terminates. This is simulated in etcd
   * using a TTL lease that is periodically refreshed. When the process dies, the refresh stops and
   * the node will be deleted after the TTL expires.
   */
  EPHEMERAL;
}
