package com.slack.astra.metadata.cache;

import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.slack.astra.metadata.core.AstraPartitioningMetadataStore;
import com.slack.astra.metadata.core.ZookeeperPartitioningMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class CacheSlotMetadataStore extends AstraPartitioningMetadataStore<CacheSlotMetadata> {
  public static final String CACHE_SLOT_ZK_PATH = "/partitioned_cacheSlot";

  /**
   * Initializes a cache slot metadata store at the CACHE_SLOT_ZK_PATH. This should be used to
   * create/update the cache slots, and for listening to all cache slot events.
   */
  public CacheSlotMetadataStore(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry)
      throws Exception {
    super(
        new ZookeeperPartitioningMetadataStore<>(
            curatorFramework,
            metadataStoreConfig.getZookeeperConfig(),
            meterRegistry,
            CreateMode.EPHEMERAL,
            new CacheSlotMetadataSerializer().toModelSerializer(),
            CACHE_SLOT_ZK_PATH),
        null, // Not using etcdStore for now
        metadataStoreConfig.getMode(),
        meterRegistry);
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
            cacheSlotMetadata.hostname,
            cacheSlotMetadata.replicaSet);
    // todo - consider refactoring this to return a completable future instead
    return JdkFutureAdapters.listenInPoolThread(
        updateAsync(updatedChunkMetadata).toCompletableFuture());
  }
}
