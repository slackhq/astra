package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.preprocessor.PreprocessorRateLimiter.BYTES_DROPPED;
import static com.slack.kaldb.preprocessor.PreprocessorRateLimiter.MESSAGES_DROPPED;
import static com.slack.kaldb.preprocessor.PreprocessorValueMapper.SERVICE_NAME_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Test;

public class PreprocessorRateLimiterTest {

  @Test
  public void shouldApplyScaledRateLimit() throws InterruptedException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    int preprocessorCount = 2;
    int maxBurstSeconds = 1;
    boolean initializeWarm = false;
    PreprocessorRateLimiter rateLimiter =
        new PreprocessorRateLimiter(
            meterRegistry, preprocessorCount, maxBurstSeconds, initializeWarm);

    String name = "rateLimiter";
    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey(SERVICE_NAME_KEY).setVStr(name).build())
            .build();

    // set the target so that we pass the first add, then fail the second
    long targetThroughput = ((long) span.toByteArray().length * preprocessorCount) + 1;
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            name,
            name,
            targetThroughput,
            List.of(new DatasetPartitionMetadata(100, 200, List.of("0"))),
            name);
    Predicate<String, Trace.Span> predicate =
        rateLimiter.createRateLimiter(List.of(datasetMetadata));

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
    PreprocessorRateLimiter rateLimiter = new PreprocessorRateLimiter(meterRegistry, 1, 1, false);

    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setKey(SERVICE_NAME_KEY)
                    .setVStr("unprovisioned_service")
                    .build())
            .build();

    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            "wrong_service",
            "wrong_service",
            Long.MAX_VALUE,
            List.of(new DatasetPartitionMetadata(100, 200, List.of("0"))),
            "wrong_service");
    Predicate<String, Trace.Span> predicate =
        rateLimiter.createRateLimiter(List.of(datasetMetadata));

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
    int maxBurstSeconds = 1;
    boolean initializeWarm = false;
    PreprocessorRateLimiter rateLimiter =
        new PreprocessorRateLimiter(
            meterRegistry, preprocessorCount, maxBurstSeconds, initializeWarm);

    String name = "rateLimiter";
    long targetThroughput = 1000;
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            name,
            name,
            targetThroughput,
            List.of(new DatasetPartitionMetadata(100, 200, List.of("0"))),
            name);
    Predicate<String, Trace.Span> predicate =
        rateLimiter.createRateLimiter(List.of(datasetMetadata));

    assertThat(predicate.test("key", Trace.Span.newBuilder().build())).isFalse();
    assertThat(predicate.test("key", null)).isFalse();
  }

  @Test
  public void shouldThrowOnInvalidConfigurations() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new PreprocessorRateLimiter(meterRegistry, 0, 1, true));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new PreprocessorRateLimiter(meterRegistry, 1, 0, true));
  }

  @Test
  public void shouldAllowBurstingOverLimitWarm() throws InterruptedException {
    String name = "rateLimiter";
    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey(SERVICE_NAME_KEY).setVStr(name).build())
            .build();

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    PreprocessorRateLimiter rateLimiter = new PreprocessorRateLimiter(meterRegistry, 1, 3, true);

    long targetThroughput = span.getSerializedSize() - 1;
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            name,
            name,
            targetThroughput,
            List.of(new DatasetPartitionMetadata(100, 200, List.of("0"))),
            name);
    Predicate<String, Trace.Span> predicate =
        rateLimiter.createRateLimiter(List.of(datasetMetadata));

    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isFalse();

    Thread.sleep(2000);

    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isFalse();
  }

  @Test
  public void shouldAllowBurstingOverLimitCold() throws InterruptedException {
    String name = "rateLimiter";
    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey(SERVICE_NAME_KEY).setVStr(name).build())
            .build();

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    PreprocessorRateLimiter rateLimiter = new PreprocessorRateLimiter(meterRegistry, 1, 3, false);

    long targetThroughput = span.getSerializedSize() - 1;
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            name,
            name,
            targetThroughput,
            List.of(new DatasetPartitionMetadata(100, 200, List.of("0"))),
            name);
    Predicate<String, Trace.Span> predicate =
        rateLimiter.createRateLimiter(List.of(datasetMetadata));

    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isFalse();

    Thread.sleep(4000);

    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isTrue();
    assertThat(predicate.test("key", span)).isFalse();
  }
}
