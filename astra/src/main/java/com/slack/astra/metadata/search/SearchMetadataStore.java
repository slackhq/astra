package com.slack.astra.metadata.search;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public class SearchMetadataStore extends AstraMetadataStore<SearchMetadata> {
  public static final String SEARCH_METADATA_STORE_ZK_PATH = "/search";

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
  }

  @Override
  public AsyncStage<Stat> updateAsync(SearchMetadata metadataNode) {
    throw new UnsupportedOperationException("Updates are not permitted for search metadata");
  }
}
