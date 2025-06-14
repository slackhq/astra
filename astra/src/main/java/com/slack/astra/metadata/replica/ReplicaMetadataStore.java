package com.slack.astra.metadata.replica;

import com.slack.astra.metadata.core.AstraPartitioningMetadataStore;
import com.slack.astra.metadata.core.EtcdCreateMode;
import com.slack.astra.metadata.core.EtcdPartitioningMetadataStore;
import com.slack.astra.metadata.core.ZookeeperPartitioningMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class ReplicaMetadataStore extends AstraPartitioningMetadataStore<ReplicaMetadata> {
  public static final String REPLICA_STORE_PATH = "/partitioned_replica";

  public ReplicaMetadataStore(
      AsyncCuratorFramework curatorFramework,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry)
      throws Exception {
    super(
        curatorFramework != null
            ? new ZookeeperPartitioningMetadataStore<>(
                curatorFramework,
                metadataStoreConfig.getZookeeperConfig(),
                meterRegistry,
                CreateMode.PERSISTENT,
                new ReplicaMetadataSerializer().toModelSerializer(),
                REPLICA_STORE_PATH)
            : null,
        etcdClient != null
            ? new EtcdPartitioningMetadataStore<>(
                etcdClient,
                metadataStoreConfig.getEtcdConfig(),
                meterRegistry,
                EtcdCreateMode.PERSISTENT,
                new ReplicaMetadataSerializer(),
                REPLICA_STORE_PATH)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "ReplicaMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);
  }
}
