package com.slack.astra.metadata.preprocessor;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class PreprocessorMetadataStore extends AstraMetadataStore<PreprocessorMetadata> {
  public static final String PREPROCESSOR_ZK_PATH = "/preprocessors";

  /**
   * Initializes a cache slot metadata store at the CACHE_SLOT_ZK_PATH. This should be used to
   * create/update the cache slots, and for listening to all cache slot events.
   */
  public PreprocessorMetadataStore(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.ZookeeperConfig zkConfig,
      boolean shouldCache) {
    super(
        curatorFramework,
        zkConfig,
        CreateMode.EPHEMERAL,
        shouldCache,
        new PreprocessorMetadataSerializer().toModelSerializer(),
        PREPROCESSOR_ZK_PATH);
  }
}
