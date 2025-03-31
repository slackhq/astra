package com.slack.astra.clusterManager;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class RedactionUpdateService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(RedactionUpdateService.class);
  private static HashMap<String, FieldRedactionMetadata> fieldRedactionsMap = new HashMap<>();
  private final FieldRedactionMetadataStore fieldRedactionMetadataStore;
  //  private final MeterRegistry meterRegistry;
  private final AstraConfigs.ManagerConfig managerConfig;
  private ScheduledFuture<?> pendingTask;
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private final AstraMetadataStoreChangeListener<FieldRedactionMetadata> listener =
      (_) -> runOneIteration();

  public RedactionUpdateService(
      FieldRedactionMetadataStore fieldRedactionMetadataStore,
      AstraConfigs.ManagerConfig managerConfig) {

    this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
    this.managerConfig = managerConfig;
    //    this.meterRegistry = meterRegistry;
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
    HashMap<String, FieldRedactionMetadata> map = new HashMap<>();
    fieldRedactionMetadataStore
        .listSync()
        .forEach(
            redaction -> {
              map.put(redaction.getName(), redaction);
            });
    fieldRedactionsMap = map;
  }

  public static HashMap<String, FieldRedactionMetadata> getFieldRedactionsMap() {
    return fieldRedactionsMap;
  }

  @Override
  protected Scheduler scheduler() {
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
