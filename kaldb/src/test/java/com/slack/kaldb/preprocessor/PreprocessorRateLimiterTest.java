package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.preprocessor.PreprocessorRateLimiter.BYTES_DROPPED;
import static com.slack.kaldb.preprocessor.PreprocessorRateLimiter.MESSAGES_DROPPED;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Test;

public class PreprocessorRateLimiterTest {

  @Test
  public void shouldApplyScaledRateLimit() throws InterruptedException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    int preprocessorCount = 2;
    PreprocessorRateLimiter rateLimiter =
        new PreprocessorRateLimiter(meterRegistry, preprocessorCount);

    String name = "rateLimiter";
    long targetThroughput = 1000;
    Predicate<String, byte[]> predicate = rateLimiter.createRateLimiter(name, targetThroughput);

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate.test("key", new byte[Math.round((float) targetThroughput / 2 - 1)]))
        .isTrue();
    assertThat(predicate.test("key", new byte[10])).isFalse();

    // rate limit is targetThroughput per second, so 1 second should refill our limit
    Thread.sleep(1000);

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate.test("key", new byte[Math.round((float) targetThroughput / 2 - 1)]))
        .isTrue();
    assertThat(predicate.test("key", new byte[10])).isFalse();

    assertThat(MetricsUtil.getCount(MESSAGES_DROPPED, meterRegistry)).isEqualTo(2);
    assertThat(MetricsUtil.getCount(BYTES_DROPPED, meterRegistry)).isEqualTo(20);
  }

  @Test
  public void shouldHandleEmptyMessages() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    int preprocessorCount = 1;
    PreprocessorRateLimiter rateLimiter =
        new PreprocessorRateLimiter(meterRegistry, preprocessorCount);

    String name = "rateLimiter";
    long targetThroughput = 1000;
    Predicate<String, byte[]> predicate = rateLimiter.createRateLimiter(name, targetThroughput);

    assertThat(predicate.test("key", new byte[0])).isTrue();
    assertThat(predicate.test("key", null)).isTrue();
  }

  @Test
  public void shouldThrowOnInvalidConfigurations() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new PreprocessorRateLimiter(meterRegistry, 0));
  }
}
