package com.slack.astra.clusterManager;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class RedactionUpdateService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(RedactionUpdateService.class);
  private static HashMap<String, FieldRedactionMetadata> fieldRedactionsMap = new HashMap<>();
  private final FieldRedactionMetadataStore fieldRedactionMetadataStore;

  private final AstraConfigs.RedactionUpdateServiceConfig redactionUpdateServiceConfig;
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  public RedactionUpdateService(
      FieldRedactionMetadataStore fieldRedactionMetadataStore,
      AstraConfigs.RedactionUpdateServiceConfig redactionUpdateServiceConfig) {

    this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
    this.redactionUpdateServiceConfig = redactionUpdateServiceConfig;
  }

  @Override
  protected synchronized void runOneIteration() {
    updateRedactionMetadataList();
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
        1, redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs(), TimeUnit.SECONDS);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Redaction Update Service");
    updateRedactionMetadataList();
  }

  @Override
  protected void shutDown() throws Exception {
    executor.shutdownNow();
    LOG.info("Closed Redaction Update Service");
  }
}
