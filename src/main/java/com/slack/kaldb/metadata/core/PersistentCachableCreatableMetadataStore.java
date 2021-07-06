package com.slack.kaldb.metadata.core;

import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;

public class PersistentCachableCreatableMetadataStore<T extends KaldbMetadata>
    extends PersistentCreatableMetadataStore<T> {

  public PersistentCachableCreatableMetadataStore(
      boolean cache,
      MetadataStore metadataStore,
      String snapshotStoreFolder,
      MetadataSerializer<T> metadataSerializer,
      Logger logger) {
    super(metadataStore, snapshotStoreFolder, metadataSerializer, logger);
  }
}
