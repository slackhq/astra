package com.slack.astra.util;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/**
 * Stateful exponential backoff with proportional jitter. Each call to {@link #nextBackOffMillis()}
 * returns a randomized delay and advances the internal interval by the multiplier. Returns {@link
 * #STOP} when the elapsed time since the first call exceeds the configured maximum.
 *
 * <p>The randomization applies ±{@code RANDOMIZATION_FACTOR} around the current interval (e.g.
 * ±50%), so jitter scales proportionally with the backoff. The multiplier advances the base
 * interval each call, and overflow is avoided by capping before multiplication.
 *
 * <p>All public methods are synchronized for safe use from concurrent gRPC callback and retry
 * threads.
 */
public class ExponentialBackOff {
  private static final double RANDOMIZATION_FACTOR = 0.5;
  private static final double MULTIPLIER = 1.5;
  public static final long STOP = -1L;

  private final long initialIntervalMillis;
  private final long maxIntervalMillis;
  private final long maxElapsedTimeMillis;
  private final LongSupplier clock;
  private long currentIntervalMillis;
  private long startTimeMs;

  public ExponentialBackOff(
      long initialIntervalMillis, long maxIntervalMillis, long maxElapsedTimeMillis) {
    this(initialIntervalMillis, maxIntervalMillis, maxElapsedTimeMillis, System::currentTimeMillis);
  }

  public ExponentialBackOff(
      long initialIntervalMillis,
      long maxIntervalMillis,
      long maxElapsedTimeMillis,
      LongSupplier clock) {
    this.initialIntervalMillis = initialIntervalMillis;
    this.maxIntervalMillis = maxIntervalMillis;
    this.maxElapsedTimeMillis = maxElapsedTimeMillis;
    this.clock = clock;
    reset();
  }

  public synchronized void reset() {
    this.currentIntervalMillis = initialIntervalMillis;
    this.startTimeMs = -1;
  }

  public synchronized long getElapsedTimeMs() {
    if (startTimeMs < 0) {
      return 0;
    }
    return clock.getAsLong() - startTimeMs;
  }

  public synchronized long nextBackOffMillis() {
    long now = clock.getAsLong();
    if (startTimeMs < 0) {
      startTimeMs = now;
    }
    if (now - startTimeMs >= maxElapsedTimeMillis) {
      return STOP;
    }

    // Randomize ±RANDOMIZATION_FACTOR around current interval
    double delta = RANDOMIZATION_FACTOR * currentIntervalMillis;
    double minInterval = currentIntervalMillis - delta;
    double maxInterval = currentIntervalMillis + delta;
    long randomized =
        (long)
            (minInterval + ThreadLocalRandom.current().nextDouble() * (maxInterval - minInterval));

    // Advance interval for next call (overflow-safe)
    if (currentIntervalMillis >= this.maxIntervalMillis / MULTIPLIER) {
      currentIntervalMillis = this.maxIntervalMillis;
    } else {
      currentIntervalMillis = (long) (currentIntervalMillis * MULTIPLIER);
    }

    return randomized;
  }
}
