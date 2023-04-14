package com.slack.kaldb.metadata.recovery;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import com.google.common.util.concurrent.ListenableFuture;
import com.slack.kaldb.metadata.core.EphemeralMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.InternalMetadataStoreException;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  public void getAndUpdateNonFreeRecoveryNodeStateSync(
      String slotName, Metadata.RecoveryNodeMetadata.RecoveryNodeState newRecoveryNodeState) {
    try {
      getAndUpdateNonFreeRecoveryNodeState(slotName, newRecoveryNodeState)
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | TimeoutException | InterruptedException e) {
      throw new InternalMetadataStoreException("Failed to update recovery node: " + slotName, e);
    }
  }

  public ListenableFuture<?> getAndUpdateNonFreeRecoveryNodeState(
      String slotName, Metadata.RecoveryNodeMetadata.RecoveryNodeState newRecoveryNodeState) {
    RecoveryNodeMetadata recoveryNode = getNodeSync(slotName);
    if (recoveryNode.recoveryNodeState.equals(
        Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)) {
      throw new IllegalArgumentException(
          "Current slot state can't be free: " + recoveryNode.toString());
    }
    String newRecoveryTaskId =
        newRecoveryNodeState.equals(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)
            ? ""
            : recoveryNode.recoveryTaskName;

    return updateRecoveryNodeState(recoveryNode, newRecoveryNodeState, newRecoveryTaskId);
  }

  /** Update RecoveryNodeState with a recovery task while keeping the remaining fields the same. */
  public ListenableFuture<?> updateRecoveryNodeState(
      final RecoveryNodeMetadata recoveryNode,
      final Metadata.RecoveryNodeMetadata.RecoveryNodeState newState,
      final String newRecoveryTaskName) {
    if (newState.equals(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)
        && !newRecoveryTaskName.isEmpty()) {
      throw new IllegalArgumentException("Recovery node in free state should have empty task name");
    }
    if (!newState.equals(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)
        && newRecoveryTaskName.isEmpty()) {
      throw new IllegalArgumentException(
          "Recovery node in non-free state should have a valid recovery task assigned");
    }
    return update(
        new RecoveryNodeMetadata(
            recoveryNode.name,
            newState,
            newRecoveryTaskName,
            recoveryNode.supportedIndexTypes,
            Instant.now().toEpochMilli()));
  }
}
