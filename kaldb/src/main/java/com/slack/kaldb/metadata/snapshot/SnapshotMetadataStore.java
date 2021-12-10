package com.slack.kaldb.metadata.snapshot;

import com.slack.kaldb.metadata.core.PersistentMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotMetadataStore extends PersistentMutableMetadataStore<SnapshotMetadata> {
  public static final String SNAPSHOT_METADATA_STORE_ZK_PATH = "/snapshot";

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotMetadataStore.class);

  // TODO: Consider restricting the update methods to only update live nodes only?

  public SnapshotMetadataStore(MetadataStore metadataStore, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        true,
        SNAPSHOT_METADATA_STORE_ZK_PATH,
        metadataStore,
        new SnapshotMetadataSerializer(),
        LOG);
  }
}
