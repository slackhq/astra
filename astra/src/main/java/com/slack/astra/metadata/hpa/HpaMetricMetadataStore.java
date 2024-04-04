package com.slack.astra.metadata.hpa;

import com.slack.astra.metadata.core.AstraMetadataStore;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class HpaMetricMetadataStore extends AstraMetadataStore<HpaMetricMetadata> {
  public static final String AUTOSCALER_METADATA_STORE_ZK_PATH = "/hpa_metrics";

  public HpaMetricMetadataStore(AsyncCuratorFramework curator, boolean shouldCache) {
    super(
        curator,
        CreateMode.EPHEMERAL,
        shouldCache,
        new HpaMetricMetadataSerializer().toModelSerializer(),
        AUTOSCALER_METADATA_STORE_ZK_PATH);
  }
}
