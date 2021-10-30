package com.slack.kaldb.metadata.snapshot;

import com.slack.kaldb.metadata.core.PersistentMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotMetadataStore extends PersistentMutableMetadataStore<SnapshotMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotMetadataStore.class);

  // TODO: Consider restricting the update methods to only update live nodes only?

  public SnapshotMetadataStore(
      MetadataStore metadataStore, String snapshotStorePath, boolean shouldCache) throws Exception {
    super(
        shouldCache, true, snapshotStorePath, metadataStore, new SnapshotMetadataSerializer(), LOG);
  }
}
