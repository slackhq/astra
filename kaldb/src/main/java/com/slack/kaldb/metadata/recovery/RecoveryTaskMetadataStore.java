package com.slack.kaldb.metadata.recovery;

import com.slack.kaldb.metadata.core.KaldbMetadataStore;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class RecoveryTaskMetadataStore extends KaldbMetadataStore<RecoveryTaskMetadata> {
  public static final String RECOVERY_TASK_ZK_PATH = "/recoveryTask";

  public RecoveryTaskMetadataStore(AsyncCuratorFramework curatorFramework, boolean shouldCache)
      throws Exception {
    super(
        curatorFramework,
        CreateMode.PERSISTENT,
        shouldCache,
        new RecoveryTaskMetadataSerializer().toModelSerializer(),
        RECOVERY_TASK_ZK_PATH);
  }
}
