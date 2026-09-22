package com.slack.astra.metadata.cache;

import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.slack.astra.metadata.core.AstraPartitioningMetadataStore;
import com.slack.astra.metadata.core.EtcdCreateMode;
import com.slack.astra.metadata.core.EtcdPartitioningMetadataStore;
import com.slack.astra.metadata.core.ZookeeperPartitioningMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class CacheNodeAssignmentStore extends AstraPartitioningMetadataStore<CacheNodeAssignment> {
  public static final String CACHE_NODE_ASSIGNMENT_STORE_PATH = "/cacheAssignment";

  /**
   * Manager nodes watch assignments for all cache nodes. A cache node must use the {@code
   * cacheNodeId} filter instead.
   */
  public CacheNodeAssignmentStore(
      AsyncCuratorFramework curator,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry) {
    this(curator, etcdClient, metadataStoreConfig, meterRegistry, List.of());
  }

  /** Watches assignments for only {@code cacheNodeId}. */
  public CacheNodeAssignmentStore(
      AsyncCuratorFramework curator,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      String cacheNodeId) {
    this(curator, etcdClient, metadataStoreConfig, meterRegistry, List.of(cacheNodeId));
  }

  /** An empty {@code partitionFilters} means watch every partition; see the public constructors. */
  private CacheNodeAssignmentStore(
      AsyncCuratorFramework curator,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      List<String> partitionFilters) {
    super(
        curator != null
            ? new ZookeeperPartitioningMetadataStore<>(
                curator,
                metadataStoreConfig.getZookeeperConfig(),
                meterRegistry,
                CreateMode.PERSISTENT,
                new CacheNodeAssignmentSerializer().toModelSerializer(),
                CACHE_NODE_ASSIGNMENT_STORE_PATH,
                partitionFilters)
            : null,
        etcdClient != null
            ? new EtcdPartitioningMetadataStore<>(
                etcdClient,
                metadataStoreConfig.getEtcdConfig(),
                meterRegistry,
                EtcdCreateMode.PERSISTENT,
                new CacheNodeAssignmentSerializer(),
                CACHE_NODE_ASSIGNMENT_STORE_PATH,
                partitionFilters)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "CacheNodeAssignmentStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
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
