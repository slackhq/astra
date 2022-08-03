package com.slack.kaldb.metadata.dataset;

import com.slack.kaldb.metadata.core.PersistentMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasetMetadataStore extends PersistentMutableMetadataStore<DatasetMetadata> {
  // TODO: The path should be dataset, but leaving it as /service for backwards compatibility.
  public static final String DATASET_METADATA_STORE_ZK_PATH = "/service";

  private static final Logger LOG = LoggerFactory.getLogger(DatasetMetadataStore.class);

  public DatasetMetadataStore(MetadataStore metadataStore, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        true,
        DATASET_METADATA_STORE_ZK_PATH,
        metadataStore,
        new DatasetMetadataSerializer(),
        LOG);
  }
}
