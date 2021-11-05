package com.slack.kaldb.metadata.recovery;

import com.slack.kaldb.metadata.core.PersistentMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryTaskMetadataStore
    extends PersistentMutableMetadataStore<RecoveryTaskMetadata> {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryTaskMetadataStore.class);
  public static final String RECOVERY_NODE_ZK_PATH = "/recoveryTask";

  public RecoveryTaskMetadataStore(MetadataStore metadataStore, boolean shouldCache)
      throws Exception {
    super(
        shouldCache,
        false,
        RECOVERY_NODE_ZK_PATH,
        metadataStore,
        new RecoveryTaskMetadataSerializer(),
        LOG);
  }
}
