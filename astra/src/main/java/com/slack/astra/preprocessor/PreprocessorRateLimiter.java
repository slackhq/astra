package com.slack.astra.preprocessor;

import static com.slack.astra.metadata.dataset.DatasetMetadata.MATCH_ALL_SERVICE;
import static com.slack.astra.metadata.dataset.DatasetMetadata.MATCH_STAR_SERVICE;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PreprocessorRateLimiter provides a thread-safe Kafka streams predicate for determining if a
 * Kafka message should be filtered or not. Metrics are provided for visibility when a rate limiter
 * is dropping messages.
 */
@ThreadSafe
@SuppressWarnings("UnstableApiUsage")
public class PreprocessorRateLimiter {
  private static final Logger LOG = LoggerFactory.getLogger(PreprocessorRateLimiter.class);

  private int preprocessorCount;

  private final int maxBurstSeconds;

  private final boolean initializeWarm;

  public static final String RATE_LIMIT_BYTES = "preprocessor_rate_limit_bytes_limit";
  public static final String MESSAGES_DROPPED = "preprocessor_rate_limit_messages_dropped";
  public static final String BYTES_DROPPED = "preprocessor_rate_limit_bytes_dropped";

  private final MultiGauge rateLimitBytesLimit;
  private final Meter.MeterProvider<Counter> messagesDroppedCounterProvider;
  private final Meter.MeterProvider<Counter> bytesDroppedCounterProvider;

  /** Span key for KeyValue pair to use as the service name */
  public static String SERVICE_NAME_KEY = "service_name";

  public enum MessageDropReason {
    MISSING_SERVICE_NAME,
    NOT_PROVISIONED,
    OVER_LIMIT
  }

  public PreprocessorRateLimiter(
      final MeterRegistry meterRegistry,
      final int preprocessorCount,
      final int maxBurstSeconds,
      final boolean initializeWarm) {
    Preconditions.checkArgument(preprocessorCount > 0, "Preprocessor count must be greater than 0");
    Preconditions.checkArgument(
        maxBurstSeconds >= 1, "Preprocessor maxBurstSeconds must be greater than or equal to 1");

    this.preprocessorCount = preprocessorCount;
    this.maxBurstSeconds = maxBurstSeconds;
    this.initializeWarm = initializeWarm;

    this.rateLimitBytesLimit =
        MultiGauge.builder(RATE_LIMIT_BYTES)
            .description("The configured rate limit per service, per indexer, in bytes")
            .register(meterRegistry);
    this.messagesDroppedCounterProvider =
        Counter.builder(MESSAGES_DROPPED)
            .description("Number of messages dropped")
            .withRegistry(meterRegistry);
    this.bytesDroppedCounterProvider =
        Counter.builder(BYTES_DROPPED)
            .description("Bytes of messages dropped")
            .withRegistry(meterRegistry);
  }

  /**
   * Creates a burstable rate limiter based on Guava rate limiting. This is supported by Guava, but
   * isn't exposed due to some philosophical arguments about needing a major refactor first -
   * https://github.com/google/guava/issues/1707.
   *
   * @param permitsPerSecond how many permits to grant per second - will require warmup period
   * @param maxBurstSeconds how many seconds permits can be accumulated - default guava value is 1s
   * @param initializeWarm if stored permits are initialized to the max value that can be
   *     accumulated
   */
  protected static RateLimiter smoothBurstyRateLimiter(
      double permitsPerSecond, double maxBurstSeconds, boolean initializeWarm) {
    try {
      Class<?> sleepingStopwatchClass =
          Class.forName("com.google.common.util.concurrent.RateLimiter$SleepingStopwatch");
      Method createFromSystemTimerMethod =
          sleepingStopwatchClass.getDeclaredMethod("createFromSystemTimer");
      createFromSystemTimerMethod.setAccessible(true);
      Object stopwatch = createFromSystemTimerMethod.invoke(null);

      Class<?> burstyRateLimiterClass =
          Class.forName("com.google.common.util.concurrent.SmoothRateLimiter$SmoothBursty");
      Constructor<?> burstyRateLimiterConstructor =
          burstyRateLimiterClass.getDeclaredConstructors()[0];
      burstyRateLimiterConstructor.setAccessible(true);

      RateLimiter result =
          (RateLimiter) burstyRateLimiterConstructor.newInstance(stopwatch, maxBurstSeconds);
      result.setRate(permitsPerSecond);

      if (initializeWarm) {
        Field storedPermitsField =
            result.getClass().getSuperclass().getDeclaredField("storedPermits");
        storedPermitsField.setAccessible(true);
        storedPermitsField.set(result, permitsPerSecond * maxBurstSeconds);
      }

      return result;
    } catch (Exception e) {
      LOG.error(
          "Error creating smooth bursty rate limiter, defaulting to non-bursty rate limiter", e);
      return RateLimiter.create(permitsPerSecond);
    }
  }

  public static int getSpanBytes(List<Trace.Span> spans) {
    return spans.stream().mapToInt(Trace.Span::getSerializedSize).sum();
  }

  public BiPredicate<String, List<Trace.Span>> createBulkIngestRateLimiter(
      List<DatasetMetadata> datasetMetadataList, Integer preprocessorCount) {
    this.preprocessorCount = preprocessorCount;
    return this.createBulkIngestRateLimiter(datasetMetadataList);
  }

  public BiPredicate<String, List<Trace.Span>> createBulkIngestRateLimiter(
      List<DatasetMetadata> datasetMetadataList) {

    List<DatasetMetadata> throughputSortedDatasets = sortDatasetsOnThroughput(datasetMetadataList);
    Map<String, RateLimiter> rateLimiterMap = getRateLimiterMap(throughputSortedDatasets);

    rateLimitBytesLimit.register(
        throughputSortedDatasets.stream()
            .map(
                datasetMetadata -> {
                  // get the currently active partition, and then calculate the active partitions
                  Optional<Integer> activePartitionCount =
                      datasetMetadata.getPartitionConfigs().stream()
                          .filter((item) -> item.getEndTimeEpochMs() == Long.MAX_VALUE)
                          .map(item -> item.getPartitions().size())
                          .findFirst();

                  return activePartitionCount
                      .map(
                          integer ->
                              MultiGauge.Row.of(
                                  Tags.of(Tag.of("service", datasetMetadata.getName())),
                                  datasetMetadata.getThroughputBytes() / integer))
                      .orElse(null);
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList()),
        true);

    return (index, docs) -> {
      if (docs == null) {
        LOG.warn("Message was dropped, was null span");
        return false;
      }

      int totalBytes = getSpanBytes(docs);
      if (index == null) {
        // index name wasn't provided
        LOG.debug("Message was dropped due to missing index name - '{}'", index);
        messagesDroppedCounterProvider
            .withTags(getMeterTags("", MessageDropReason.MISSING_SERVICE_NAME))
            .increment(docs.size());
        bytesDroppedCounterProvider
            .withTags(getMeterTags("", MessageDropReason.MISSING_SERVICE_NAME))
            .increment(totalBytes);
        return false;
      }
      for (DatasetMetadata datasetMetadata : throughputSortedDatasets) {
        String serviceNamePattern = datasetMetadata.getServiceNamePattern();
        // back-compat since this is a new field
        if (serviceNamePattern == null) {
          serviceNamePattern = datasetMetadata.getName();
        }

        if (serviceNamePattern.equals(MATCH_ALL_SERVICE)
            || serviceNamePattern.equals(MATCH_STAR_SERVICE)
            || index.equals(serviceNamePattern)) {
          RateLimiter rateLimiter = rateLimiterMap.get(datasetMetadata.getName());
          if (rateLimiter.tryAcquire(totalBytes)) {
            return true;
          }
          // message should be dropped due to rate limit
          messagesDroppedCounterProvider
              .withTags(getMeterTags(index, MessageDropReason.OVER_LIMIT))
              .increment(docs.size());
          bytesDroppedCounterProvider
              .withTags(getMeterTags(index, MessageDropReason.OVER_LIMIT))
              .increment(totalBytes);
          LOG.debug(
              "Message was dropped for dataset '{}' due to rate limiting ({} bytes per second)",
              index,
              rateLimiter.getRate());
          return false;
        }
      }
      // message should be dropped due to no matching service name being provisioned
      messagesDroppedCounterProvider
          .withTags(getMeterTags(index, MessageDropReason.NOT_PROVISIONED))
          .increment(docs.size());
      bytesDroppedCounterProvider
          .withTags(getMeterTags(index, MessageDropReason.NOT_PROVISIONED))
          .increment(totalBytes);
      return false;
    };
  }

  public Map<String, RateLimiter> getRateLimiterMap(
      List<DatasetMetadata> throughputSortedDatasets) {

    return throughputSortedDatasets.stream()
        .collect(
            Collectors.toMap(
                DatasetMetadata::getName,
                datasetMetadata -> {
                  double permitsPerSecond =
                      (double) datasetMetadata.getThroughputBytes() / preprocessorCount;
                  LOG.info(
                      "Rate limiter initialized for {} at {} bytes per second (target throughput {} / processorCount {})",
                      datasetMetadata.getName(),
                      permitsPerSecond,
                      datasetMetadata.getThroughputBytes(),
                      preprocessorCount);
                  return smoothBurstyRateLimiter(permitsPerSecond, maxBurstSeconds, initializeWarm);
                }));
  }

  private static List<Tag> getMeterTags(String serviceName, MessageDropReason reason) {
    return List.of(Tag.of("service", serviceName), Tag.of("reason", reason.toString()));
  }

  // we sort the datasets to rank from which dataset do we start matching candidate service names
  // in the future we can change the ordering from sort to something else
  public static List<DatasetMetadata> sortDatasetsOnThroughput(
      List<DatasetMetadata> datasetMetadataList) {
    return datasetMetadataList.stream()
        .sorted(Comparator.comparingLong(DatasetMetadata::getThroughputBytes).reversed())
        .collect(Collectors.toList());
  }
}
