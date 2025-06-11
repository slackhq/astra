package com.slack.astra.metadata.search;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.InternalMetadataStoreException;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class SearchMetadataStore extends AstraMetadataStore<SearchMetadata> {
  public static final String SEARCH_METADATA_STORE_ZK_PATH = "/search";
  private final AstraConfigs.MetadataStoreConfig metadataStoreConfig;

  public SearchMetadataStore(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    super(
        new ZookeeperMetadataStore<>(
            curatorFramework,
            metadataStoreConfig.getZookeeperConfig(),
            CreateMode.EPHEMERAL,
            shouldCache,
            new SearchMetadataSerializer().toModelSerializer(),
            SEARCH_METADATA_STORE_ZK_PATH,
            meterRegistry),
        null, // Not using etcdStore for now
        metadataStoreConfig.getMode(),
        meterRegistry);
    this.metadataStoreConfig = metadataStoreConfig;
  }

  // ONLY updating the `searchable` field is allowed on SearchMetadata.
  // This is needed to gate queries from hitting cache nodes until they're fully hydrated
  public void updateSearchability(SearchMetadata oldSearchMetadata, boolean searchable) {
    oldSearchMetadata.setSearchable(searchable);
    try {
      super.updateAsync(oldSearchMetadata)
          .toCompletableFuture()
          .get(
              metadataStoreConfig.hasZookeeperConfig()
                  ? metadataStoreConfig.getZookeeperConfig().getZkConnectionTimeoutMs()
                  : 1000,
              TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error updating node: " + oldSearchMetadata, e);
    }
  }

  @Override
  public CompletionStage<String> updateAsync(SearchMetadata metadataNode) {
    throw new UnsupportedOperationException("Updates are not permitted for search metadata");
  }

  @Override
  public void updateSync(SearchMetadata metadataNode) {
    throw new UnsupportedOperationException("Updates are not permitted for search metadata");
  }
}
