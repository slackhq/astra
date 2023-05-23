package com.slack.kaldb.metadata.cache;

import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.slack.kaldb.metadata.core.KaldbMetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class CacheSlotMetadataStore extends KaldbMetadataStore<CacheSlotMetadata> {
  public static final String CACHE_SLOT_ZK_PATH = "/cacheSlot";

  /**
   * Initializes a cache slot metadata store at the CACHE_SLOT_ZK_PATH. This should be used to
   * create/update the cache slots, and for listening to all cache slot events.
   */
  public CacheSlotMetadataStore(AsyncCuratorFramework curatorFramework, boolean shouldCache)
      throws Exception {
    super(
        curatorFramework,
        CreateMode.EPHEMERAL,
        shouldCache,
        new CacheSlotMetadataSerializer().toModelSerializer(),
        CACHE_SLOT_ZK_PATH);
  }

  /**
   * Initializes a cache slot metadata store at CACHE_SLOT_ZK_PATH/{cacheSlotName}. This should be
   * used to add listeners to specific cache slots, and is not expected to be used for mutating any
   * nodes.
   */
  public CacheSlotMetadataStore(
      AsyncCuratorFramework curatorFramework, String cacheSlotName, boolean shouldCache)
      throws Exception {
    super(
        curatorFramework,
        CreateMode.EPHEMERAL,
        shouldCache,
        new CacheSlotMetadataSerializer().toModelSerializer(),
        String.format("%s/%s", CACHE_SLOT_ZK_PATH, cacheSlotName));
  }

  /** Fetch the node given a slotName and update the slot state. */
  public ListenableFuture<?> getAndUpdateNonFreeCacheSlotState(
      String slotName, Metadata.CacheSlotMetadata.CacheSlotState slotState) {
    CacheSlotMetadata cacheSlotMetadata = getSync(slotName);
    return updateNonFreeCacheSlotState(cacheSlotMetadata, slotState);
  }

  /** Update the cache slot state, if the slot is not FREE. */
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

  /** Update CacheSlotState and replicaId fields while keeping the remaining fields the same. */
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
    // todo - consider refactoring this to return a completable future instead
    return JdkFutureAdapters.listenInPoolThread(
        updateAsync(updatedChunkMetadata).toCompletableFuture());
  }
}
