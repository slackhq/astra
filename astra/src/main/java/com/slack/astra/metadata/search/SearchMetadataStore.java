package com.slack.astra.metadata.search;

import com.slack.astra.metadata.core.AstraPartitioningMetadataStore;
import com.slack.astra.metadata.core.InternalMetadataStoreException;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class SearchMetadataStore extends AstraPartitioningMetadataStore<SearchMetadata> {
  public static final String SEARCH_METADATA_STORE_ZK_PATH = "/partitioned_search";
  private final AstraConfigs.ZookeeperConfig zkConfig;

  public SearchMetadataStore(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.ZookeeperConfig zkConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache)
      throws Exception {
    super(
        curatorFramework,
        zkConfig,
        meterRegistry,
        CreateMode.EPHEMERAL,
        new SearchMetadataSerializer().toModelSerializer(),
        SEARCH_METADATA_STORE_ZK_PATH);
    this.zkConfig = zkConfig;
  }

  // ONLY updating the `searchable` field is allowed on SearchMetadata.
  // This is needed to gate queries from hitting cache nodes until they're fully hydrated
  public void updateSearchability(SearchMetadata oldSearchMetadata, boolean searchable) {
    oldSearchMetadata.setSearchable(searchable);
    try {
      super.updateAsync(oldSearchMetadata)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error updating node: " + oldSearchMetadata, e);
    }
  }
}
