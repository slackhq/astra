package com.slack.kaldb.metadata.replica;

import com.slack.kaldb.metadata.core.PersistentMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaMetadataStore extends PersistentMutableMetadataStore<ReplicaMetadata> {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaMetadataStore.class);
  public static final String REPLICA_STORE_ZK_PATH = "/replica";

  public ReplicaMetadataStore(MetadataStore metadataStore, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        false,
        REPLICA_STORE_ZK_PATH,
        metadataStore,
        new ReplicaMetadataSerializer(),
        LOG);
  }
}
