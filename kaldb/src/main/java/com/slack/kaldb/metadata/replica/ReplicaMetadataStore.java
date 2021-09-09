package com.slack.kaldb.metadata.replica;

import com.slack.kaldb.metadata.core.PersistentMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaMetadataStore extends PersistentMutableMetadataStore<ReplicaMetadata> {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaMetadataStore.class);

  public ReplicaMetadataStore(
      MetadataStore metadataStore, String snapshotStorePath, boolean shouldCache) throws Exception {
    super(
        shouldCache, false, snapshotStorePath, metadataStore, new ReplicaMetadataSerializer(), LOG);
  }
}
