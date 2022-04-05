package com.slack.kaldb.preprocessor;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.List;
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

  public static final String MESSAGES_DROPPED = "preprocessor_rate_limit_messages_dropped";
  public static final String BYTES_DROPPED = "preprocessor_rate_limit_bytes_dropped";

  public PreprocessorRateLimiter(final MeterRegistry meterRegistry, final int preprocessorCount) {
    Preconditions.checkArgument(preprocessorCount > 0, "Preprocessor count must be greater than 0");

    this.meterRegistry = meterRegistry;
    this.preprocessorCount = preprocessorCount;
  }

  public Predicate<String, byte[]> createRateLimiter(
      final String name, final long throughputBytes) {
    double permitsPerSecond = (double) throughputBytes / preprocessorCount;
    LOG.info(
        "Rate limiter initialized for {} at {} bytes per second (target throughput {} / processorCount {})",
        name,
        permitsPerSecond,
        throughputBytes,
        preprocessorCount);
    RateLimiter rateLimiter = RateLimiter.create(permitsPerSecond);

    Iterable<Tag> tags = List.of(Tag.of("service", name));
    Counter messagesDropped = meterRegistry.counter(MESSAGES_DROPPED, tags);
    Counter bytesDropped = meterRegistry.counter(BYTES_DROPPED, tags);

    return (String key, byte[] value) -> {
      if (value == null || value.length == 0) {
        return true;
      } else if (rateLimiter.tryAcquire(value.length)) {
        return true;
      } else {
        messagesDropped.increment();
        bytesDropped.increment(value.length);
        LOG.warn(
            "Message was dropped from topic {} due to rate limiting ({} bytes per second), wanted {} bytes",
            name,
            permitsPerSecond,
            value.length);
        return false;
      }
    };
  }
}
