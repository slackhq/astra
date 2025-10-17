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

public class RecoveryTaskMetadataStore extends AstraMetadataStore<RecoveryTaskMetadata> {
  public static final String RECOVERY_TASK_PATH = "/recoveryTask";

  public RecoveryTaskMetadataStore(
      AsyncCuratorFramework curatorFramework,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache)
      throws Exception {
    super(
        curatorFramework != null
            ? new ZookeeperMetadataStore<>(
                curatorFramework,
                metadataStoreConfig.getZookeeperConfig(),
                CreateMode.PERSISTENT,
                shouldCache,
                new RecoveryTaskMetadataSerializer().toModelSerializer(),
                RECOVERY_TASK_PATH,
                meterRegistry)
            : null,
        etcdClient != null
            ? new EtcdMetadataStore<>(
                RECOVERY_TASK_PATH,
                metadataStoreConfig.getEtcdConfig(),
                shouldCache,
                meterRegistry,
                new RecoveryTaskMetadataSerializer(),
                EtcdCreateMode.PERSISTENT,
                etcdClient)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "RecoveryTaskMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);
  }
}
