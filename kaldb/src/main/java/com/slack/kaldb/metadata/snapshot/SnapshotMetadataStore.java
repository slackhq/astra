package com.slack.kaldb.metadata.snapshot;

import com.slack.kaldb.metadata.core.KaldbMetadataStore;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class SnapshotMetadataStore extends KaldbMetadataStore<SnapshotMetadata> {
  public static final String SNAPSHOT_METADATA_STORE_ZK_PATH = "/snapshot";

  // TODO: Consider restricting the update methods to only update live nodes only?

  public SnapshotMetadataStore(AsyncCuratorFramework curatorFramework, boolean shouldCache)
      throws Exception {
    super(
        curatorFramework,
        CreateMode.PERSISTENT,
        shouldCache,
        new SnapshotMetadataSerializer().toModelSerializer(),
        SNAPSHOT_METADATA_STORE_ZK_PATH);
  }
}
