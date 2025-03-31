package com.slack.astra.clusterManager;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class RedactionService extends AbstractScheduledService {
  private static final Logger LOG =
      LoggerFactory.getLogger(com.slack.astra.server.HpaMetricPublisherService.class);
  private HashMap<String, FieldRedactionMetadata> fieldRedactionsMap;
  private final FieldRedactionMetadataStore fieldRedactionMetadataStore;
  private final MeterRegistry meterRegistry;
  private final AstraConfigs.ManagerConfig managerConfig;
  private ScheduledFuture<?> pendingTask;
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private final AstraMetadataStoreChangeListener<FieldRedactionMetadata> listener =
      (_) -> runOneIteration();

  public RedactionService(
      FieldRedactionMetadataStore fieldRedactionMetadataStore,
      AstraConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {

    this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
    this.managerConfig = managerConfig;
    this.meterRegistry = meterRegistry;
    this.fieldRedactionsMap = new HashMap<>();
  }

  @Override
  protected synchronized void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executor.schedule(
              this::updateRedactionMetadataList,
              this.managerConfig.getEventAggregationSecs(),
              TimeUnit.SECONDS);
    }
  }

  private void updateRedactionMetadataList() {
    fieldRedactionMetadataStore
        .listSync()
        .forEach(
            redaction -> {
              fieldRedactionsMap.put(redaction.getName(), redaction);
            });
  }

  public HashMap<String, FieldRedactionMetadata> getFieldRedactionsMap() {
      return fieldRedactionsMap;
  }

  @Override Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(
              1, managerConfig.getEventAggregationSecs(), TimeUnit.SECONDS);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Redaction Service");
    updateRedactionMetadataList();
    fieldRedactionMetadataStore.addListener(listener);
  }

  @Override
  protected void shutDown() throws Exception {
    fieldRedactionMetadataStore.removeListener(listener);
    executor.shutdownNow();
    LOG.info("Closed Redaction Service");
  }
}
