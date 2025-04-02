package com.slack.astra.metadata.replica;

import com.slack.astra.metadata.core.AstraPartitioningMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class ReplicaMetadataStore extends AstraPartitioningMetadataStore<ReplicaMetadata> {
  public static final String REPLICA_STORE_ZK_PATH = "/partitioned_replica";

  public ReplicaMetadataStore(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.ZookeeperConfig zkConfig,
      MeterRegistry meterRegistry)
      throws Exception {
    super(
        curatorFramework,
        zkConfig,
        meterRegistry,
        CreateMode.PERSISTENT,
        new ReplicaMetadataSerializer().toModelSerializer(),
        REPLICA_STORE_ZK_PATH);
  }
}
