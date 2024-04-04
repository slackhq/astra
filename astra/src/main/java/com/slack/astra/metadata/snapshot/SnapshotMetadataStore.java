package com.slack.astra.metadata.snapshot;

import com.slack.astra.metadata.core.AstraPartitioningMetadataStore;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class SnapshotMetadataStore extends AstraPartitioningMetadataStore<SnapshotMetadata> {
  public static final String SNAPSHOT_METADATA_STORE_ZK_PATH = "/partitioned_snapshot";

  // TODO: Consider restricting the update methods to only update live nodes only?

  public SnapshotMetadataStore(AsyncCuratorFramework curatorFramework) throws Exception {
    super(
        curatorFramework,
        CreateMode.PERSISTENT,
        new SnapshotMetadataSerializer().toModelSerializer(),
        SNAPSHOT_METADATA_STORE_ZK_PATH);
  }
}
