package com.slack.astra.metadata.recovery;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class RecoveryTaskMetadataStore extends AstraMetadataStore<RecoveryTaskMetadata> {
  public static final String RECOVERY_TASK_ZK_PATH = "/recoveryTask";

  public RecoveryTaskMetadataStore(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.ZookeeperConfig zkConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache)
      throws Exception {
    super(
        curatorFramework,
        zkConfig,
        CreateMode.PERSISTENT,
        shouldCache,
        new RecoveryTaskMetadataSerializer().toModelSerializer(),
        RECOVERY_TASK_ZK_PATH,
        meterRegistry);
  }
}
