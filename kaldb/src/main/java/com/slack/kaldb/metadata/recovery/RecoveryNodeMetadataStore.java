package com.slack.kaldb.metadata.recovery;

import com.slack.kaldb.metadata.core.EphemeralMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryNodeMetadataStore extends EphemeralMutableMetadataStore<RecoveryNodeMetadata> {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryNodeMetadataStore.class);
  public static final String RECOVERY_NODE_ZK_PATH = "/recoveryNode";

  public RecoveryNodeMetadataStore(MetadataStore metadataStore, boolean shouldCache)
      throws Exception {
    super(
        shouldCache,
        false,
        RECOVERY_NODE_ZK_PATH,
        metadataStore,
        new RecoveryNodeMetadataSerializer(),
        LOG);
  }
}
