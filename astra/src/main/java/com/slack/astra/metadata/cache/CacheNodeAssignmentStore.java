package com.slack.astra.metadata.cache;

import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.slack.astra.metadata.core.AstraPartitioningMetadataStore;
import com.slack.astra.proto.metadata.Metadata;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class CacheNodeAssignmentStore extends AstraPartitioningMetadataStore<CacheNodeAssignment> {
  public static final String CACHE_NODE_ASSIGNMENT_STORE_ZK_PATH = "/cacheAssignment";

  public CacheNodeAssignmentStore(AsyncCuratorFramework curator) {
    super(
        curator,
        CreateMode.PERSISTENT,
        new CacheNodeAssignmentSerializer().toModelSerializer(),
        CACHE_NODE_ASSIGNMENT_STORE_ZK_PATH);
  }

  public ListenableFuture<?> updateAssignmentState(
      final CacheNodeAssignment cacheNodeAssignment,
      final Metadata.CacheNodeAssignment.CacheNodeAssignmentState state) {
    CacheNodeAssignment updatedCacheNodeAssignment =
        new CacheNodeAssignment(
            cacheNodeAssignment.assignmentId,
            cacheNodeAssignment.cacheNodeId,
            cacheNodeAssignment.snapshotId,
            cacheNodeAssignment.replicaSet,
            state);

    return JdkFutureAdapters.listenInPoolThread(
        updateAsync(updatedCacheNodeAssignment).toCompletableFuture());
  }
}
