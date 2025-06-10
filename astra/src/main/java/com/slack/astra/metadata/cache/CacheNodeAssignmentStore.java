package com.slack.astra.metadata.cache;

import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.slack.astra.metadata.core.AstraPartitioningMetadataStore;
import com.slack.astra.metadata.core.ZookeeperPartitioningMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class CacheNodeAssignmentStore extends AstraPartitioningMetadataStore<CacheNodeAssignment> {
  public static final String CACHE_NODE_ASSIGNMENT_STORE_ZK_PATH = "/cacheAssignment";

  public CacheNodeAssignmentStore(
      AsyncCuratorFramework curator,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry) {
    super(
        new ZookeeperPartitioningMetadataStore<>(
            curator,
            metadataStoreConfig.getZookeeperConfig(),
            meterRegistry,
            CreateMode.PERSISTENT,
            new CacheNodeAssignmentSerializer().toModelSerializer(),
            CACHE_NODE_ASSIGNMENT_STORE_ZK_PATH),
        null, // Not using etcdStore for now
        metadataStoreConfig.getMode(),
        meterRegistry);
  }

  /** Restricts the cache node assignment store to only watching events for cacheNodeId */
  public CacheNodeAssignmentStore(
      AsyncCuratorFramework curator,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      String cacheNodeId) {
    super(
        new ZookeeperPartitioningMetadataStore<>(
            curator,
            metadataStoreConfig.getZookeeperConfig(),
            meterRegistry,
            CreateMode.PERSISTENT,
            new CacheNodeAssignmentSerializer().toModelSerializer(),
            CACHE_NODE_ASSIGNMENT_STORE_ZK_PATH,
            List.of(cacheNodeId)),
        null, // Not using etcdStore for now
        metadataStoreConfig.getMode(),
        meterRegistry);
  }

  public ListenableFuture<?> updateAssignmentState(
      final CacheNodeAssignment cacheNodeAssignment,
      final Metadata.CacheNodeAssignment.CacheNodeAssignmentState state) {
    CacheNodeAssignment updatedCacheNodeAssignment =
        new CacheNodeAssignment(
            cacheNodeAssignment.assignmentId,
            cacheNodeAssignment.cacheNodeId,
            cacheNodeAssignment.snapshotId,
            cacheNodeAssignment.replicaId,
            cacheNodeAssignment.replicaSet,
            cacheNodeAssignment.snapshotSize,
            state);

    return JdkFutureAdapters.listenInPoolThread(
        updateAsync(updatedCacheNodeAssignment).toCompletableFuture());
  }
}
