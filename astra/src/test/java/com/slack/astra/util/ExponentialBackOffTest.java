package com.slack.astra.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

public class ExponentialBackOffTest {

  @Test
  public void testExponentialBackOff() {
    // initialInterval=1000, maxInterval=10000, maxElapsed=60000
    ExponentialBackOff backoff = new ExponentialBackOff(1000, 10000, 60000);

    // First call should return a value randomized around the initial interval (±50%)
    for (int trial = 0; trial < 20; trial++) {
      backoff.reset();
      long delay = backoff.nextBackOffMillis();
      // With ±50% randomization around 1000ms: range is [500, 1500]
      assertThat(delay).isBetween(500L, 1500L);
    }

    // Verify the progression grows with the 1.5x multiplier
    backoff.reset();
    for (int i = 0; i < 5; i++) {
      long delay = backoff.nextBackOffMillis();
      assertThat(delay).isNotEqualTo(ExponentialBackOff.STOP);
      // Each call's range center should be >= previous (on average)
      // Just verify the delay is positive and within max bounds
      assertThat(delay).isGreaterThan(0L);
      // Max possible is maxInterval * 1.5 (±50% of capped interval)
      assertThat(delay).isLessThanOrEqualTo(15000L);
    }
  }

  @Test
  public void testExponentialBackOffCapsAtMax() {
    // initialInterval=5000, maxInterval=10000 — should cap quickly
    ExponentialBackOff backoff = new ExponentialBackOff(5000, 10000, 60000);

    backoff.nextBackOffMillis(); // interval advances to 7500
    backoff.nextBackOffMillis(); // interval advances to 10000 (capped)
    // Third call should be randomized around 10000: [5000, 15000]
    for (int trial = 0; trial < 20; trial++) {
      ExponentialBackOff b = new ExponentialBackOff(5000, 10000, 60000);
      b.nextBackOffMillis();
      b.nextBackOffMillis();
      long delay = b.nextBackOffMillis();
      assertThat(delay).isBetween(5000L, 15000L);
    }
  }

  @Test
  public void testExponentialBackOffStopsAfterBudget() {
    // Use a controllable clock so the test doesn't depend on wall-clock time
    AtomicLong fakeClock = new AtomicLong(0);
    ExponentialBackOff backoff = new ExponentialBackOff(100, 500, 200, fakeClock::get);

    // First call starts the timer at t=0, should succeed
    long delay = backoff.nextBackOffMillis();
    assertThat(delay).isNotEqualTo(ExponentialBackOff.STOP);

    // Advance clock past budget
    fakeClock.set(250);
    delay = backoff.nextBackOffMillis();
    assertThat(delay).isEqualTo(ExponentialBackOff.STOP);
    assertThat(backoff.getElapsedTimeMs()).isGreaterThanOrEqualTo(200);
  }

  @Test
  public void testExponentialBackOffReset() {
    AtomicLong fakeClock = new AtomicLong(0);
    ExponentialBackOff backoff = new ExponentialBackOff(1000, 10000, 60000, fakeClock::get);

    // Advance a few times, simulating time passing
    backoff.nextBackOffMillis();
    fakeClock.set(5000);
    backoff.nextBackOffMillis();
    assertThat(backoff.getElapsedTimeMs()).isGreaterThan(0);

    // Reset should bring it back to initial state
    backoff.reset();
    assertThat(backoff.getElapsedTimeMs()).isEqualTo(0);

    // Next call should be around initial interval again
    long delay = backoff.nextBackOffMillis();
    assertThat(delay).isBetween(500L, 1500L);
  }
}
