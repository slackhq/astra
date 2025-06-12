package com.slack.astra.metadata.recovery;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class RecoveryTaskMetadataStore extends AstraMetadataStore<RecoveryTaskMetadata> {
  public static final String RECOVERY_TASK_ZK_PATH = "/recoveryTask";

  public RecoveryTaskMetadataStore(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache)
      throws Exception {
    super(
        new ZookeeperMetadataStore<>(
            curatorFramework,
            metadataStoreConfig.getZookeeperConfig(),
            CreateMode.PERSISTENT,
            shouldCache,
            new RecoveryTaskMetadataSerializer().toModelSerializer(),
            RECOVERY_TASK_ZK_PATH,
            meterRegistry),
        null, // Not using etcdStore for now
        metadataStoreConfig.getMode(),
        meterRegistry);
  }
}
