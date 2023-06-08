package com.slack.kaldb.metadata.replica;

import com.slack.kaldb.metadata.core.KaldbMetadataStore;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class ReplicaMetadataStore extends KaldbMetadataStore<ReplicaMetadata> {
  public static final String REPLICA_STORE_ZK_PATH = "/replica";

  public ReplicaMetadataStore(AsyncCuratorFramework curatorFramework, boolean shouldCache)
      throws Exception {
    super(
        curatorFramework,
        CreateMode.PERSISTENT,
        shouldCache,
        new ReplicaMetadataSerializer().toModelSerializer(),
        REPLICA_STORE_ZK_PATH);
  }
}
