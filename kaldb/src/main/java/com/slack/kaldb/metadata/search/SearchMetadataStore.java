package com.slack.kaldb.metadata.search;

import com.slack.kaldb.metadata.core.EphemeralMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchMetadataStore extends EphemeralMutableMetadataStore<SearchMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(SearchMetadataStore.class);

  public SearchMetadataStore(MetadataStore metadataStore, String storeFolder, boolean shouldCache)
      throws Exception {
    super(shouldCache, false, storeFolder, metadataStore, new SearchMetadataSerializer(), LOG);
  }
}
