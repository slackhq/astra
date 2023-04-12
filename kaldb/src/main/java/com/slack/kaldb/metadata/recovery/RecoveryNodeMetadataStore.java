package com.slack.kaldb.metadata.recovery;

import com.slack.kaldb.metadata.core.EphemeralMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryNodeMetadataStore extends EphemeralMutableMetadataStore<RecoveryNodeMetadata> {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryNodeMetadataStore.class);
  public static final String RECOVERY_NODE_ZK_PATH = "/recoveryNode";

  /**
   * Initializes a recovery node metadata store at the RECOVERY_NODE_ZK_PATH. This should be used to
   * create/update the recovery nodes, and for listening to all recovery node events.
   */
  public RecoveryNodeMetadataStore(MetadataStore metadataStore, boolean shouldCache)
      throws Exception {
    super(
        shouldCache,
        true,
        RECOVERY_NODE_ZK_PATH,
        metadataStore,
        new RecoveryNodeMetadataSerializer(),
        LOG);
  }

  /**
   * Initializes a recovery node metadata store at RECOVERY_NODE_ZK_PATH/{recoveryNodeName}. This
   * should be used to add listeners to specific recovery nodes, and is not expected to be used for
   * mutating any nodes.
   */
  public RecoveryNodeMetadataStore(
      MetadataStore metadataStore, String recoveryNodeName, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        false,
        String.format("%s/%s", RECOVERY_NODE_ZK_PATH, recoveryNodeName),
        metadataStore,
        new RecoveryNodeMetadataSerializer(),
        LOG);
  }

  public void getAndUpdateRecoveryNodeState(
      String slotName, Metadata.RecoveryNodeMetadata.RecoveryNodeState newRecoveryNodeState) {
    RecoveryNodeMetadata recoveryNodeMetadata = getNodeSync(slotName);
    RecoveryNodeMetadata updatedRecoveryNodeMetadata =
        new RecoveryNodeMetadata(
            recoveryNodeMetadata.name,
            newRecoveryNodeState,
            newRecoveryNodeState.equals(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)
                ? ""
                : recoveryNodeMetadata.recoveryTaskName,
            recoveryNodeMetadata.supportedIndexTypes,
            Instant.now().toEpochMilli());
    updateSync(updatedRecoveryNodeMetadata);
  }
}
