package com.slack.astra.metadata.search;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.EtcdCreateMode;
import com.slack.astra.metadata.core.EtcdMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.CompletionStage;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class SearchMetadataStoreLegacy extends AstraMetadataStore<SearchMetadata> {
  public static final String SEARCH_METADATA_STORE_PATH = "/search";

  public SearchMetadataStoreLegacy(
      AsyncCuratorFramework curatorFramework,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    super(
        curatorFramework != null
            ? new ZookeeperMetadataStore<>(
                curatorFramework,
                metadataStoreConfig.getZookeeperConfig(),
                CreateMode.EPHEMERAL,
                shouldCache,
                new SearchMetadataSerializer().toModelSerializer(),
                SEARCH_METADATA_STORE_PATH,
                meterRegistry)
            : null,
        etcdClient != null
            ? new EtcdMetadataStore<>(
                SEARCH_METADATA_STORE_PATH,
                metadataStoreConfig.getEtcdConfig(),
                shouldCache,
                meterRegistry,
                new SearchMetadataSerializer(),
                EtcdCreateMode.EPHEMERAL,
                etcdClient)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "SearchMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);
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
