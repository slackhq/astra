package com.slack.kaldb.metadata.cache;

import com.google.common.util.concurrent.ListenableFuture;
import com.slack.kaldb.metadata.core.EphemeralMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheSlotMetadataStore extends EphemeralMutableMetadataStore<CacheSlotMetadata> {
  private static final int TIMEOUT_MS = 5000;

  private static final Logger LOG = LoggerFactory.getLogger(CacheSlotMetadataStore.class);
  public static final String CACHE_SLOT_ZK_PATH = "/cacheSlot";

  /**
   * Initializes a cache slot metadata store at the CACHE_SLOT_ZK_PATH. This should be used to
   * create/update the cache slots, and for listening to all cache slot events.
   */
  public CacheSlotMetadataStore(MetadataStore metadataStore, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        true,
        CACHE_SLOT_ZK_PATH,
        metadataStore,
        new CacheSlotMetadataSerializer(),
        LOG);
  }

  /**
   * Initializes a cache slot metadata store at CACHE_SLOT_ZK_PATH/{cacheSlotName}. This should be
   * used to add listeners to specific cache slots, and is not expected to be used for mutating any
   * nodes.
   */
  public CacheSlotMetadataStore(
      MetadataStore metadataStore, String cacheSlotName, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        false,
        String.format("%s/%s", CACHE_SLOT_ZK_PATH, cacheSlotName),
        metadataStore,
        new CacheSlotMetadataSerializer(),
        LOG);
  }

  // Fetch the node given a slotName and update the slot state.
  public boolean updateNonFreeCacheSlotStateSync(
      String slotName, Metadata.CacheSlotMetadata.CacheSlotState slotState) {
    try {
      CacheSlotMetadata cacheSlotMetadata = getNodeSync(slotName);
      updateNonFreeCacheSlotState(cacheSlotMetadata, slotState)
          .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return true;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Error setting chunk metadata state");
      return false;
    }
  }

  // Update the cache slot state, if the slot is not FREE.
  public ListenableFuture<?> updateNonFreeCacheSlotState(
      final CacheSlotMetadata cacheSlotMetadata,
      final Metadata.CacheSlotMetadata.CacheSlotState slotState) {
    if (cacheSlotMetadata.cacheSlotState.equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE)) {
      throw new IllegalArgumentException(
          "Current state of slot can't be free: " + cacheSlotMetadata.name);
    }
    String replicaId =
        slotState.equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE)
            ? ""
            : cacheSlotMetadata.replicaId;
    return updateCacheSlotStateStateWithReplicaId(cacheSlotMetadata, slotState, replicaId);
  }

  // Update CacheSlotState and replicaId fields while keeping the remaining fields the same.
  public ListenableFuture<?> updateCacheSlotStateStateWithReplicaId(
      final CacheSlotMetadata cacheSlotMetadata,
      final Metadata.CacheSlotMetadata.CacheSlotState newState,
      final String replicaId) {
    CacheSlotMetadata updatedChunkMetadata =
        new CacheSlotMetadata(
            cacheSlotMetadata.name,
            newState,
            replicaId,
            Instant.now().toEpochMilli(),
            cacheSlotMetadata.supportedIndexTypes);
    return update(updatedChunkMetadata);
  }

  // TODO: Add unit tests in CacheSlotMetadataStore setChunkMetadata state.
}
