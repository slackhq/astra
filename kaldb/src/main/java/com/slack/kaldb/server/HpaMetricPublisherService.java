package com.slack.kaldb.server;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.hpa.HpaMetricMetadata;
import com.slack.kaldb.metadata.hpa.HpaMetricMetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service reads stored HPA (horizontal pod autoscaler) metrics from Zookeeper as calculated by
 * the manager node, and then reports these as pod-level metrics.
 */
public class HpaMetricPublisherService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(HpaMetricPublisherService.class);
  private final HpaMetricMetadataStore hpaMetricMetadataStore;
  private final Metadata.HpaMetricMetadata.NodeRole nodeRole;
  private final MeterRegistry meterRegistry;
  private final KaldbMetadataStoreChangeListener<HpaMetricMetadata> listener = changeListener();

  public HpaMetricPublisherService(
      HpaMetricMetadataStore hpaMetricMetadataStore,
      MeterRegistry meterRegistry,
      Metadata.HpaMetricMetadata.NodeRole nodeRole) {
    this.hpaMetricMetadataStore = hpaMetricMetadataStore;
    this.nodeRole = nodeRole;
    this.meterRegistry = meterRegistry;
  }

  private KaldbMetadataStoreChangeListener<HpaMetricMetadata> changeListener() {
    return metadata -> {
      if (metadata.getNodeRole().equals(nodeRole)) {
        meterRegistry.gauge(
            metadata.getName(),
            hpaMetricMetadataStore,
            store -> {
              Optional<HpaMetricMetadata> metric =
                  store.listSync().stream()
                      .filter(m -> m.getName().equals(metadata.getName()))
                      .findFirst();
              if (metric.isPresent()) {
                return metric.get().getValue();
              } else {
                // store no longer has this metric - report a nominal value 1
                return 1;
              }
            });
      }
    };
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting autoscaler publisher service");
    hpaMetricMetadataStore.addListener(listener);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping autoscaler publisher service");
    hpaMetricMetadataStore.removeListener(listener);
  }
}
