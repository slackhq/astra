package com.slack.kaldb.preprocessor;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.streams.kstream.Predicate;
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

  private final MeterRegistry meterRegistry;
  private final int preprocessorCount;
  private final Duration rateLimitDuration;

  public static final String MESSAGES_DROPPED = "preprocessor_rate_limit_messages_dropped";
  public static final String BYTES_DROPPED = "preprocessor_rate_limit_bytes_dropped";

  public enum MessageDropReason {
    MISSING_SERVICE_NAME,
    NOT_PROVISIONED,
    OVER_LIMIT
  }

  public PreprocessorRateLimiter(
      final MeterRegistry meterRegistry,
      final int preprocessorCount,
      final long rateLimitSmoothingMicros) {
    Preconditions.checkArgument(preprocessorCount > 0, "Preprocessor count must be greater than 0");
    Preconditions.checkArgument(
        rateLimitSmoothingMicros >= 0,
        "Preprocessor rateLimitSmoothingMicros must be greater than or equal to 0");

    this.meterRegistry = meterRegistry;
    this.preprocessorCount = preprocessorCount;
    this.rateLimitDuration = Duration.of(rateLimitSmoothingMicros, ChronoUnit.MICROS);
  }

  public Predicate<String, Trace.Span> createRateLimiter(
      Map<String, Long> serviceNameToThroughput) {
    Map<String, RateLimiter> rateLimiterMap =
        serviceNameToThroughput
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    (Map.Entry::getKey),
                    (entry -> {
                      double permitsPerSecond = (double) entry.getValue() / preprocessorCount;
                      LOG.info(
                          "Rate limiter initialized for {} at {} bytes per second (target throughput {} / processorCount {})",
                          entry.getKey(),
                          permitsPerSecond,
                          entry.getValue(),
                          preprocessorCount);
                      return RateLimiter.create(permitsPerSecond);
                    })));

    return (key, value) -> {
      if (value == null) {
        LOG.warn("Message was dropped, was null span");
        return false;
      }

      String serviceName = PreprocessorValueMapper.getServiceName(value);
      int bytes = value.getSerializedSize();
      if (serviceName == null || serviceName.isEmpty()) {
        // service name wasn't provided
        LOG.debug("Message was dropped due to missing service name - '{}'", value);
        // todo - we may consider adding a logging BurstFilter so that a bad actor cannot
        //  inadvertently swamp the system if we want to increase this logging level
        //  https://logging.apache.org/log4j/2.x/manual/filters.html#BurstFilter
        meterRegistry
            .counter(MESSAGES_DROPPED, getMeterTags("", MessageDropReason.MISSING_SERVICE_NAME))
            .increment();
        meterRegistry
            .counter(BYTES_DROPPED, getMeterTags("", MessageDropReason.MISSING_SERVICE_NAME))
            .increment(bytes);
        return false;
      }

      if (!rateLimiterMap.containsKey(serviceName)) {
        // service isn't provisioned in our rate limit map
        meterRegistry
            .counter(MESSAGES_DROPPED, getMeterTags(serviceName, MessageDropReason.NOT_PROVISIONED))
            .increment();
        meterRegistry
            .counter(BYTES_DROPPED, getMeterTags(serviceName, MessageDropReason.NOT_PROVISIONED))
            .increment(bytes);
        LOG.debug(
            "Message was dropped from service '{}' as it not currently provisioned", serviceName);
        return false;
      }

      if (rateLimiterMap.get(serviceName).tryAcquire(bytes, rateLimitDuration)) {
        return true;
      }

      // message should be dropped due to rate limit
      meterRegistry
          .counter(MESSAGES_DROPPED, getMeterTags(serviceName, MessageDropReason.OVER_LIMIT))
          .increment();
      meterRegistry
          .counter(BYTES_DROPPED, getMeterTags(serviceName, MessageDropReason.OVER_LIMIT))
          .increment(bytes);
      LOG.debug(
          "Message was dropped from service '{}' due to rate limiting ({} bytes per second), wanted {} bytes",
          serviceName,
          rateLimiterMap.get(serviceName).getRate(),
          serviceName);
      return false;
    };
  }

  private static List<Tag> getMeterTags(String serviceName, MessageDropReason reason) {
    return List.of(Tag.of("service", serviceName), Tag.of("reason", reason.toString()));
  }
}
