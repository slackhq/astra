package com.slack.kaldb.metadata.snapshot;

import com.slack.kaldb.metadata.core.PersistentMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotMetadataStore extends PersistentMutableMetadataStore<SnapshotMetadata> {
  public static final String SNAPSHOT_METADATA_STORE_ZK_PATH = "/snapshots";

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotMetadataStore.class);

  // TODO: Add a setup method to initialize the store?

  public SnapshotMetadataStore(MetadataStore metadataStore, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        false,
        SNAPSHOT_METADATA_STORE_ZK_PATH,
        metadataStore,
        new SnapshotMetadataSerializer(),
        LOG);
  }
}
