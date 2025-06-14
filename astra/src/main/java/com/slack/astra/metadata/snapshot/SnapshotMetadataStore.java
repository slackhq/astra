package com.slack.astra.metadata.snapshot;

import com.slack.astra.metadata.core.AstraPartitioningMetadataStore;
import com.slack.astra.metadata.core.EtcdCreateMode;
import com.slack.astra.metadata.core.EtcdPartitioningMetadataStore;
import com.slack.astra.metadata.core.ZookeeperPartitioningMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class SnapshotMetadataStore extends AstraPartitioningMetadataStore<SnapshotMetadata> {
  public static final String SNAPSHOT_METADATA_STORE_PATH = "/partitioned_snapshot";

  // TODO: Consider restricting the update methods to only update live nodes only?

  public SnapshotMetadataStore(
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
                new SnapshotMetadataSerializer().toModelSerializer(),
                SNAPSHOT_METADATA_STORE_PATH)
            : null,
        etcdClient != null
            ? new EtcdPartitioningMetadataStore<>(
                etcdClient,
                metadataStoreConfig.getEtcdConfig(),
                meterRegistry,
                EtcdCreateMode.PERSISTENT,
                new SnapshotMetadataSerializer(),
                SNAPSHOT_METADATA_STORE_PATH)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "SnapshotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);
  }
}
