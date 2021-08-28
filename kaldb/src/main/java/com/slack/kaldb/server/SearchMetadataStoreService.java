package com.slack.kaldb.server;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import java.util.concurrent.TimeUnit;

public class SearchMetadataStoreService extends AbstractIdleService {
  public static String SEARCH_METADATA_PATH = "/search";
  private final MetadataStoreService metadataStoreService;
  private final boolean shouldCache;
  private SearchMetadataStore searchStore;

  public SearchMetadataStoreService(
      MetadataStoreService metadataStoreService, boolean shouldCache) {
    this.metadataStoreService = metadataStoreService;
    this.shouldCache = shouldCache;
  }

  @Override
  protected void startUp() throws Exception {
    metadataStoreService.awaitRunning(15, TimeUnit.SECONDS);
    searchStore =
        new SearchMetadataStore(
            metadataStoreService.getMetadataStore(), SEARCH_METADATA_PATH, shouldCache);
  }

  @Override
  protected void shutDown() throws Exception {
    searchStore.close();
  }
}
