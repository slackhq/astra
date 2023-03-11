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

  public boolean setChunkMetadataStateSync(
      String slotName, Metadata.CacheSlotMetadata.CacheSlotState newChunkState) {
    try {
      CacheSlotMetadata chunkMetadata = getNodeSync(slotName);
      setChunkMetadataState(chunkMetadata, newChunkState).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return true;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Error setting chunk metadata state");
      return false;
    }
  }

  public ListenableFuture<?> setChunkMetadataState(
      CacheSlotMetadata chunkMetadata, Metadata.CacheSlotMetadata.CacheSlotState newChunkState) {
    CacheSlotMetadata updatedChunkMetadata =
        new CacheSlotMetadata(
            chunkMetadata.name,
            newChunkState,
            newChunkState.equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE)
                ? ""
                : chunkMetadata.replicaId,
            Instant.now().toEpochMilli(),
            chunkMetadata.supportedIndexTypes);
    return update(updatedChunkMetadata);
  }

  // TODO: Add unit tests in CacheSlotMetadataStore setChunkMetadata state.
}
