package com.slack.kaldb.metadata.snapshot;

import com.slack.kaldb.metadata.core.PersistentCreatableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotMetadataStore extends PersistentCreatableMetadataStore<SnapshotMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotMetadataStore.class);

  // TODO: Add a setup method to initialize the store?

  // TODO: Implement watches in underlying ZK store.
  public SnapshotMetadataStore(MetadataStore metadataStore, String snapshotStoreFolder) {
    super(metadataStore, snapshotStoreFolder, new SnapshotMetadataSerializer(), LOG);
  }
}
