package com.slack.astra.bulkIngestApi;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.preprocessor.PreprocessorRateLimiter;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * Guava service that maintains an rate limiting object consistent with the value stored in the
 * dataset metadata store.
 */
public class DatasetRateLimitingService extends AbstractIdleService {
  private final DatasetMetadataStore datasetMetadataStore;
  private final AstraMetadataStoreChangeListener<DatasetMetadata> datasetListener =
      (_) -> updateRateLimiter();

  private final PreprocessorRateLimiter rateLimiter;
  private BiPredicate<String, List<Trace.Span>> rateLimiterPredicate;

  private final PrometheusMeterRegistry meterRegistry;
  public static final String RATE_LIMIT_RELOAD_TIMER =
      "preprocessor_dataset_rate_limit_reload_timer";
  private final Timer rateLimitReloadtimer;

  public DatasetRateLimitingService(
      DatasetMetadataStore datasetMetadataStore,
      AstraConfigs.PreprocessorConfig preprocessorConfig,
      PrometheusMeterRegistry meterRegistry) {
    this.datasetMetadataStore = datasetMetadataStore;
    this.meterRegistry = meterRegistry;

    this.rateLimiter =
        new PreprocessorRateLimiter(
            meterRegistry,
            preprocessorConfig.getPreprocessorInstanceCount(),
            preprocessorConfig.getRateLimiterMaxBurstSeconds(),
            true);

    this.rateLimitReloadtimer = meterRegistry.timer(RATE_LIMIT_RELOAD_TIMER);
  }

  private void updateRateLimiter() {
    Timer.Sample sample = Timer.start(meterRegistry);
    try {
      List<DatasetMetadata> datasetMetadataList = datasetMetadataStore.listSync();
      this.rateLimiterPredicate = rateLimiter.createBulkIngestRateLimiter(datasetMetadataList);
    } finally {
      // TODO: re-work this so that we can add success/failure tags and capture them
      sample.stop(rateLimitReloadtimer);
    }
  }

  @Override
  protected void startUp() throws Exception {
    updateRateLimiter();
    datasetMetadataStore.addListener(datasetListener);
  }

  @Override
  protected void shutDown() throws Exception {
    datasetMetadataStore.removeListener(datasetListener);
  }

  public boolean tryAcquire(String index, List<Trace.Span> value) {
    return rateLimiterPredicate.test(index, value);
  }
}
