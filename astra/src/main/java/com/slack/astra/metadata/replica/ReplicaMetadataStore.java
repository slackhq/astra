package com.slack.astra.metadata.replica;

import com.slack.astra.metadata.core.AstraPartitioningMetadataStore;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class ReplicaMetadataStore extends AstraPartitioningMetadataStore<ReplicaMetadata> {
  public static final String REPLICA_STORE_ZK_PATH = "/partitioned_replica";

  public ReplicaMetadataStore(AsyncCuratorFramework curatorFramework) throws Exception {
    super(
        curatorFramework,
        CreateMode.PERSISTENT,
        new ReplicaMetadataSerializer().toModelSerializer(),
        REPLICA_STORE_ZK_PATH);
  }
}
