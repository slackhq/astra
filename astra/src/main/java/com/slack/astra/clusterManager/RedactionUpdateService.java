package com.slack.astra.clusterManager;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RedactionUpdateService updates the redaction hashmap every x minutes (default 5) which is then
 * used in searches. This is to prevent excessive calls to ZK on every search. This service runs on
 * cache and indexer nodes.
 */
public class RedactionUpdateService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(RedactionUpdateService.class);
  private static HashMap<String, FieldRedactionMetadata> fieldRedactionsMap = new HashMap<>();
  private final FieldRedactionMetadataStore fieldRedactionMetadataStore;
  private final AtomicInteger redactionUpdatesGauge;

  private final AstraConfigs.RedactionUpdateServiceConfig redactionUpdateServiceConfig;

  public RedactionUpdateService(
      FieldRedactionMetadataStore fieldRedactionMetadataStore,
      AstraConfigs.RedactionUpdateServiceConfig redactionUpdateServiceConfig,
      MeterRegistry meterRegistry) {

    this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
    this.redactionUpdateServiceConfig = redactionUpdateServiceConfig;

    this.redactionUpdatesGauge =
        meterRegistry.gauge("astra_redactions_list_size", new AtomicInteger(0));
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
    this.redactionUpdatesGauge.set(map.size());
    fieldRedactionsMap = map;
  }

  public static HashMap<String, FieldRedactionMetadata> getFieldRedactionsMap() {
    return fieldRedactionsMap;
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        redactionUpdateServiceConfig.getRedactionUpdateInitDelaySecs(),
        redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs(),
        TimeUnit.SECONDS);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Redaction Update Service");
    updateRedactionMetadataList();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closed Redaction Update Service");
  }
}
