package com.slack.kaldb.metadata.search;

import com.slack.kaldb.metadata.core.EphemeralMutableMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchMetadataStore extends EphemeralMutableMetadataStore<SearchMetadata> {
  public static final String SEARCH_METADATA_STORE_ZK_PATH = "/search";

  private static final Logger LOG = LoggerFactory.getLogger(SearchMetadataStore.class);

  public SearchMetadataStore(MetadataStore metadataStore, boolean shouldCache) throws Exception {
    super(
        shouldCache,
        false,
        SEARCH_METADATA_STORE_ZK_PATH,
        metadataStore,
        new SearchMetadataSerializer(),
        LOG);
  }
}
