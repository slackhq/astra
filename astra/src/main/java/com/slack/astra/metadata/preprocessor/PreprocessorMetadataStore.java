package com.slack.astra.metadata.preprocessor;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class PreprocessorMetadataStore extends AstraMetadataStore<PreprocessorMetadata> {
  public static final String PREPROCESSOR_ZK_PATH = "/preprocessors";

  /**
   * Initializes a preprocessor metadata store at the PREPROCESSOR_ZK_PATH. This should be used to
   * create/update the preprocessors, and for listening to all preprocessor events.
   */
  public PreprocessorMetadataStore(
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
            new PreprocessorMetadataSerializer().toModelSerializer(),
            PREPROCESSOR_ZK_PATH,
            meterRegistry),
        null, // Not using etcdStore for now
        metadataStoreConfig.getMode(),
        meterRegistry);
  }
}
