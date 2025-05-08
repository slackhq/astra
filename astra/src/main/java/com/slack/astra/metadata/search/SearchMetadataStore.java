package com.slack.astra.metadata.search;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.InternalMetadataStoreException;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SearchMetadataStore extends AstraMetadataStore<SearchMetadata> {
  public static final String SEARCH_METADATA_STORE_ZK_PATH = "/search";
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
        CreateMode.EPHEMERAL,
        shouldCache,
        new SearchMetadataSerializer().toModelSerializer(),
        SEARCH_METADATA_STORE_ZK_PATH,
        meterRegistry);
    this.zkConfig = zkConfig;
  }

  // TODO FOR KYLE: DON'T DO THIS -- DELETE AND CREATE A NEW ONE
  // Actually, because they're ephemerals created on the cache nodes, wouldn't that break the ephemeral bits?
  // Like, wouldn't that cause them to not go away when the cache nodes have disappeared, and instead tie them to the
  // managers?
  // TODO FOR KYLE: Does just updating these remove where the ephemeral is tied to? I don't think so, but we dont want that
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

  @Override
  public AsyncStage<Stat> updateAsync(SearchMetadata metadataNode) {
    throw new UnsupportedOperationException("Updates are not permitted for search metadata");
  }
}
