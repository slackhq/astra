package com.slack.astra.metadata.hpa;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class HpaMetricMetadataStore extends AstraMetadataStore<HpaMetricMetadata> {
  public static final String AUTOSCALER_METADATA_STORE_ZK_PATH = "/hpa_metrics";

  public HpaMetricMetadataStore(
      AsyncCuratorFramework curator,
      AstraConfigs.ZookeeperConfig zkConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    super(
        curator,
        zkConfig,
        CreateMode.EPHEMERAL,
        shouldCache,
        new HpaMetricMetadataSerializer().toModelSerializer(),
        AUTOSCALER_METADATA_STORE_ZK_PATH,
        meterRegistry);
  }
}
