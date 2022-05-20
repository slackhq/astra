package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.preprocessor.PreprocessorRateLimiter.BYTES_DROPPED;
import static com.slack.kaldb.preprocessor.PreprocessorRateLimiter.MESSAGES_DROPPED;
import static com.slack.kaldb.preprocessor.PreprocessorValueMapper.SERVICE_NAME_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Test;

public class PreprocessorRateLimiterTest {

  @Test
  public void shouldApplyScaledRateLimit() throws InterruptedException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    int preprocessorCount = 2;
    PreprocessorRateLimiter rateLimiter =
        new PreprocessorRateLimiter(meterRegistry, preprocessorCount, 0);

    String name = "rateLimiter";
    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey(SERVICE_NAME_KEY).setVStr(name).build())
            .build();

    // set the target so that we pass the first add, then fail the second
    long targetThroughput = ((long) span.toByteArray().length * preprocessorCount) + 1;
    Predicate<String, Trace.Span> predicate =
        rateLimiter.createRateLimiter(Map.of(name, targetThroughput));

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isFalse();

    // rate limit is targetThroughput per second, so 1 second should refill our limit
    Thread.sleep(1000);

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isFalse();

    assertThat(
            meterRegistry
                .get(MESSAGES_DROPPED)
                .tag("reason", String.valueOf(PreprocessorRateLimiter.MessageDropReason.OVER_LIMIT))
                .counter()
                .count())
        .isEqualTo(2);
    assertThat(
            meterRegistry
                .get(BYTES_DROPPED)
                .tag("reason", String.valueOf(PreprocessorRateLimiter.MessageDropReason.OVER_LIMIT))
                .counter()
                .count())
        .isEqualTo(span.toByteArray().length * 2);
  }

  @Test
  public void shouldDropMessagesWithNoConfiguration() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    PreprocessorRateLimiter rateLimiter = new PreprocessorRateLimiter(meterRegistry, 1, 0);

    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setKey(SERVICE_NAME_KEY)
                    .setVStr("unprovisioned_service")
                    .build())
            .build();
    Predicate<String, Trace.Span> predicate =
        rateLimiter.createRateLimiter(Map.of("provisioned_service", Long.MAX_VALUE));

    // this should be immediately dropped
    assertThat(predicate.test("key", span)).isFalse();

    assertThat(
            meterRegistry
                .get(MESSAGES_DROPPED)
                .tag(
                    "reason",
                    String.valueOf(PreprocessorRateLimiter.MessageDropReason.NOT_PROVISIONED))
                .counter()
                .count())
        .isEqualTo(1);
    assertThat(
            meterRegistry
                .get(BYTES_DROPPED)
                .tag(
                    "reason",
                    String.valueOf(PreprocessorRateLimiter.MessageDropReason.NOT_PROVISIONED))
                .counter()
                .count())
        .isEqualTo(span.toByteArray().length);
  }

  @Test
  public void shouldHandleEmptyMessages() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    int preprocessorCount = 1;
    PreprocessorRateLimiter rateLimiter =
        new PreprocessorRateLimiter(meterRegistry, preprocessorCount, 0);

    String name = "rateLimiter";
    long targetThroughput = 1000;
    Predicate<String, Trace.Span> predicate =
        rateLimiter.createRateLimiter(Map.of(name, targetThroughput));

    assertThat(predicate.test("key", Trace.Span.newBuilder().build())).isFalse();
    assertThat(predicate.test("key", null)).isFalse();
  }

  @Test
  public void shouldThrowOnInvalidConfigurations() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new PreprocessorRateLimiter(meterRegistry, 0, 0));
  }

  @Test
  public void shouldAllowRateLimitSmoothingOptions() {
    String name = "rateLimiter";
    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey(SERVICE_NAME_KEY).setVStr(name).build())
            .build();

    long rateLimitSmoothingMicros = 750000;
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    PreprocessorRateLimiter rateLimiter =
        new PreprocessorRateLimiter(meterRegistry, 1, rateLimitSmoothingMicros);

    long targetThroughput = (span.getSerializedSize() * 2L) - 1;
    Predicate<String, Trace.Span> predicate =
        rateLimiter.createRateLimiter(Map.of(name, targetThroughput));

    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isTrue();
  }
}
