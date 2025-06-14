package com.slack.astra.metadata.recovery;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.EtcdCreateMode;
import com.slack.astra.metadata.core.EtcdMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class RecoveryNodeMetadataStore extends AstraMetadataStore<RecoveryNodeMetadata> {
  public static final String RECOVERY_NODE_PATH = "/recoveryNode";

  /**
   * Initializes a recovery node metadata store at the RECOVERY_NODE_ZK_PATH. This should be used to
   * create/update the recovery nodes, and for listening to all recovery node events.
   */
  public RecoveryNodeMetadataStore(
      AsyncCuratorFramework curatorFramework,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    super(
        curatorFramework != null
            ? new ZookeeperMetadataStore<>(
                curatorFramework,
                metadataStoreConfig.getZookeeperConfig(),
                CreateMode.EPHEMERAL,
                shouldCache,
                new RecoveryNodeMetadataSerializer().toModelSerializer(),
                RECOVERY_NODE_PATH,
                meterRegistry)
            : null,
        etcdClient != null
            ? new EtcdMetadataStore<>(
                RECOVERY_NODE_PATH,
                metadataStoreConfig.getEtcdConfig(),
                shouldCache,
                meterRegistry,
                new RecoveryNodeMetadataSerializer(),
                EtcdCreateMode.EPHEMERAL,
                etcdClient)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "RecoveryNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);
  }

  /**
   * Initializes a recovery node metadata store at RECOVERY_NODE_ZK_PATH/{recoveryNodeName}. This
   * should be used to add listeners to specific recovery nodes, and is not expected to be used for
   * mutating any nodes.
   */
  public RecoveryNodeMetadataStore(
      AsyncCuratorFramework curatorFramework,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      String recoveryNodeName,
      boolean shouldCache) {
    super(
        curatorFramework != null
            ? new ZookeeperMetadataStore<>(
                curatorFramework,
                metadataStoreConfig.getZookeeperConfig(),
                CreateMode.EPHEMERAL,
                shouldCache,
                new RecoveryNodeMetadataSerializer().toModelSerializer(),
                String.format("%s/%s", RECOVERY_NODE_PATH, recoveryNodeName),
                meterRegistry)
            : null,
        etcdClient != null
            ? new EtcdMetadataStore<>(
                String.format("%s/%s", RECOVERY_NODE_PATH, recoveryNodeName),
                metadataStoreConfig.getEtcdConfig(),
                shouldCache,
                meterRegistry,
                new RecoveryNodeMetadataSerializer(),
                EtcdCreateMode.EPHEMERAL,
                etcdClient)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "RecoveryNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);
  }
}
