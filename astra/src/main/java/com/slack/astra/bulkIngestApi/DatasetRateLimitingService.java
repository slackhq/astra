package com.slack.astra.bulkIngestApi;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.preprocessor.PreprocessorMetadata;
import com.slack.astra.metadata.preprocessor.PreprocessorMetadataStore;
import com.slack.astra.preprocessor.PreprocessorRateLimiter;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

/**
 * Guava service that maintains a rate limiting object consistent with the value stored in the
 * dataset metadata store.
 */
public class DatasetRateLimitingService extends AbstractScheduledService {
  private final DatasetMetadataStore datasetMetadataStore;
  private final PreprocessorMetadataStore preprocessorMetadataStore;
  private final AstraMetadataStoreChangeListener<DatasetMetadata> datasetListener =
      (_) -> runOneIteration();
  private final AstraMetadataStoreChangeListener<PreprocessorMetadata> preprocessorListener =
      (_) -> runOneIteration();

  private final PreprocessorRateLimiter rateLimiter;
  private ScheduledFuture<?> pendingTask;
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private BiPredicate<String, List<Trace.Span>> rateLimiterPredicate;

  private final MeterRegistry meterRegistry;
  public static final String RATE_LIMIT_RELOAD_TIMER =
      "preprocessor_dataset_rate_limit_reload_timer";
  private final Timer rateLimitReloadtimer;
  private final AstraConfigs.PreprocessorConfig preprocessorConfig;

  public DatasetRateLimitingService(
      DatasetMetadataStore datasetMetadataStore,
      PreprocessorMetadataStore preprocessorMetadataStore,
      AstraConfigs.PreprocessorConfig preprocessorConfig,
      MeterRegistry meterRegistry) {
    this.datasetMetadataStore = datasetMetadataStore;
    this.meterRegistry = meterRegistry;
    this.preprocessorMetadataStore = preprocessorMetadataStore;
    this.preprocessorConfig = preprocessorConfig;
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
    Integer preprocessorCountValue = 1;
    try {
      List<PreprocessorMetadata> preprocessorMetadataList =
          this.preprocessorMetadataStore.listSync();
      preprocessorCountValue = preprocessorMetadataList.size();
    } catch (Exception e) {
      sample.stop(rateLimitReloadtimer);
      return;
    }

    try {
      List<DatasetMetadata> datasetMetadataList = datasetMetadataStore.listSync();

      this.rateLimiterPredicate =
          rateLimiter.createBulkIngestRateLimiter(datasetMetadataList, preprocessorCountValue);
    } finally {
      // TODO: re-work this so that we can add success/failure tags and capture them
      sample.stop(rateLimitReloadtimer);
    }
  }

  @Override
  protected synchronized void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executor.schedule(
              this::updateRateLimiter,
              this.preprocessorConfig.getDatasetRateLimitAggregationSecs(),
              TimeUnit.SECONDS);
    }
  }

  @Override
  protected void startUp() throws Exception {
    datasetMetadataStore.addListener(datasetListener);
    this.preprocessorMetadataStore.addListener(preprocessorListener);
    this.preprocessorMetadataStore.createSync(new PreprocessorMetadata());
  }

  @Override
  protected void shutDown() throws Exception {
    datasetMetadataStore.removeListener(datasetListener);
    this.preprocessorMetadataStore.removeListener(preprocessorListener);
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        1, this.preprocessorConfig.getDatasetRateLimitPeriodSecs(), TimeUnit.SECONDS);
  }

  public boolean tryAcquire(String index, List<Trace.Span> value) {
    return rateLimiterPredicate.test(index, value);
  }
}
