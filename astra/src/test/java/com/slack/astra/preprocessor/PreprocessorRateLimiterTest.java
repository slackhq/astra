package com.slack.astra.preprocessor;

import static com.slack.astra.preprocessor.PreprocessorRateLimiter.BYTES_DROPPED;
import static com.slack.astra.preprocessor.PreprocessorRateLimiter.MESSAGES_DROPPED;
import static com.slack.astra.preprocessor.PreprocessorRateLimiter.RATE_LIMIT_BYTES;
import static com.slack.astra.preprocessor.PreprocessorRateLimiter.SERVICE_NAME_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.function.BiPredicate;
import org.junit.jupiter.api.Test;

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
            List.of(new DatasetPartitionMetadata(100, Long.MAX_VALUE, List.of("0"))),
            DatasetMetadata.MATCH_ALL_SERVICE);
    BiPredicate<String, List<Trace.Span>> predicate =
        rateLimiter.createBulkIngestRateLimiter(List.of(datasetMetadata));

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isFalse();

    // rate limit is targetThroughput per second, so 1 second should refill our limit
    Thread.sleep(1000);

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isFalse();

    assertThat(
            meterRegistry
                .get(RATE_LIMIT_BYTES)
                .tag("service", datasetMetadata.getName())
                .gauge()
                .value())
        .isEqualTo(targetThroughput);
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
  public void shouldApplyScaledRateLimitWithAllServices() throws InterruptedException {
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

    // Check if _all works
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            name,
            name,
            targetThroughput,
            List.of(new DatasetPartitionMetadata(100, Long.MAX_VALUE, List.of("0"))),
            DatasetMetadata.MATCH_ALL_SERVICE);
    BiPredicate<String, List<Trace.Span>> predicate =
        rateLimiter.createBulkIngestRateLimiter(List.of(datasetMetadata));

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isFalse();

    // rate limit is targetThroughput per second, so 1 second should refill our limit
    Thread.sleep(1000);

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isFalse();

    assertThat(
            meterRegistry
                .get(RATE_LIMIT_BYTES)
                .tag("service", datasetMetadata.getName())
                .gauge()
                .value())
        .isEqualTo(targetThroughput);
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

    // Check if * works
    MeterRegistry meterRegistry1 = new SimpleMeterRegistry();
    PreprocessorRateLimiter rateLimiter1 =
        new PreprocessorRateLimiter(
            meterRegistry1, preprocessorCount, maxBurstSeconds, initializeWarm);
    DatasetMetadata datasetMetadata1 =
        new DatasetMetadata(
            name,
            name,
            targetThroughput,
            List.of(new DatasetPartitionMetadata(100, Long.MAX_VALUE, List.of("0"))),
            DatasetMetadata.MATCH_STAR_SERVICE);
    BiPredicate<String, List<Trace.Span>> predicate1 =
        rateLimiter1.createBulkIngestRateLimiter(List.of(datasetMetadata1));

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate1.test("key", List.of(span))).isTrue();
    assertThat(predicate1.test("key", List.of(span))).isFalse();

    // rate limit is targetThroughput per second, so 1 second should refill our limit
    Thread.sleep(1000);

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate1.test("key", List.of(span))).isTrue();
    assertThat(predicate1.test("key", List.of(span))).isFalse();

    assertThat(
            meterRegistry1
                .get(RATE_LIMIT_BYTES)
                .tag("service", datasetMetadata.getName())
                .gauge()
                .value())
        .isEqualTo(targetThroughput);
    assertThat(
            meterRegistry1
                .get(MESSAGES_DROPPED)
                .tag("reason", String.valueOf(PreprocessorRateLimiter.MessageDropReason.OVER_LIMIT))
                .counter()
                .count())
        .isEqualTo(2);
    assertThat(
            meterRegistry1
                .get(BYTES_DROPPED)
                .tag("reason", String.valueOf(PreprocessorRateLimiter.MessageDropReason.OVER_LIMIT))
                .counter()
                .count())
        .isEqualTo(span.toByteArray().length * 2);

    // check back compat where service name will be null
    MeterRegistry meterRegistry2 = new SimpleMeterRegistry();
    PreprocessorRateLimiter rateLimiter2 =
        new PreprocessorRateLimiter(
            meterRegistry2, preprocessorCount, maxBurstSeconds, initializeWarm);
    DatasetMetadata datasetMetadata2 =
        new DatasetMetadata(
            name,
            name,
            targetThroughput,
            List.of(new DatasetPartitionMetadata(100, Long.MAX_VALUE, List.of("0"))),
            "key");
    BiPredicate<String, List<Trace.Span>> predicate2 =
        rateLimiter2.createBulkIngestRateLimiter(List.of(datasetMetadata2));

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate2.test("key", List.of(span))).isTrue();
    assertThat(predicate2.test("key", List.of(span))).isFalse();

    // rate limit is targetThroughput per second, so 1 second should refill our limit
    Thread.sleep(1000);

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate2.test("key", List.of(span))).isTrue();
    assertThat(predicate2.test("key", List.of(span))).isFalse();

    assertThat(
            meterRegistry2
                .get(RATE_LIMIT_BYTES)
                .tag("service", datasetMetadata.getName())
                .gauge()
                .value())
        .isEqualTo(targetThroughput);
    assertThat(
            meterRegistry2
                .get(MESSAGES_DROPPED)
                .tag("reason", String.valueOf(PreprocessorRateLimiter.MessageDropReason.OVER_LIMIT))
                .counter()
                .count())
        .isEqualTo(2);
    assertThat(
            meterRegistry2
                .get(BYTES_DROPPED)
                .tag("reason", String.valueOf(PreprocessorRateLimiter.MessageDropReason.OVER_LIMIT))
                .counter()
                .count())
        .isEqualTo(span.toByteArray().length * 2);
  }

  @Test
  public void shouldApplyRateLimitsAgainstMultipleDatasets() throws InterruptedException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    int preprocessorCount = 2;
    int maxBurstSeconds = 1;
    boolean initializeWarm = false;
    PreprocessorRateLimiter rateLimiter =
        new PreprocessorRateLimiter(
            meterRegistry, preprocessorCount, maxBurstSeconds, initializeWarm);

    String name1 = "rateLimiter1";
    String name2 = "rateLimiter2";
    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey(SERVICE_NAME_KEY).setVStr(name2).build())
            .build();

    // set the target so that we pass the first add, then fail the second
    long targetThroughput = ((long) span.toByteArray().length * preprocessorCount) + 1;

    // Dataset 1 has service name will never match the record while dataset 2 will match
    DatasetMetadata datasetMetadata1 =
        new DatasetMetadata(
            name1,
            name1,
            targetThroughput,
            List.of(new DatasetPartitionMetadata(100, Long.MAX_VALUE, List.of("0"))),
            "no_service_matching_docs");
    DatasetMetadata datasetMetadata2 =
        new DatasetMetadata(
            name2,
            name2,
            targetThroughput,
            List.of(new DatasetPartitionMetadata(100, Long.MAX_VALUE, List.of("0"))),
            DatasetMetadata.MATCH_ALL_SERVICE);

    // ensure we always drop for dataset1
    BiPredicate<String, List<Trace.Span>> predicate1 =
        rateLimiter.createBulkIngestRateLimiter(List.of(datasetMetadata1));
    assertThat(predicate1.test("key", List.of(span))).isFalse();

    BiPredicate<String, List<Trace.Span>> predicate2 =
        rateLimiter.createBulkIngestRateLimiter(List.of(datasetMetadata1, datasetMetadata2));

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate2.test("key", List.of(span))).isTrue();
    assertThat(predicate2.test("key", List.of(span))).isFalse();

    // rate limit is targetThroughput per second, so 1 second should refill our limit
    Thread.sleep(1000);

    // try to get just below the scaled limit, then try to go over
    assertThat(predicate2.test("key", List.of(span))).isTrue();
    assertThat(predicate2.test("key", List.of(span))).isFalse();

    assertThat(
            meterRegistry
                .get(RATE_LIMIT_BYTES)
                // .tag("service", datasetMetadata.getName())
                .gauge()
                .value())
        .isEqualTo(targetThroughput);
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
            List.of(new DatasetPartitionMetadata(100, Long.MAX_VALUE, List.of("0"))),
            "wrong_service");
    BiPredicate<String, List<Trace.Span>> predicate =
        rateLimiter.createBulkIngestRateLimiter(List.of(datasetMetadata));

    // this should be immediately dropped
    assertThat(predicate.test("key", List.of(span))).isFalse();

    assertThat(
            meterRegistry
                .get(RATE_LIMIT_BYTES)
                .tag("service", datasetMetadata.getName())
                .gauge()
                .value())
        .isEqualTo(Long.MAX_VALUE);
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
            List.of(new DatasetPartitionMetadata(100, Long.MAX_VALUE, List.of("0"))),
            name);
    BiPredicate<String, List<Trace.Span>> predicate =
        rateLimiter.createBulkIngestRateLimiter(List.of(datasetMetadata));

    assertThat(predicate.test("key", List.of(Trace.Span.newBuilder().build()))).isFalse();
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
            List.of(new DatasetPartitionMetadata(100, Long.MAX_VALUE, List.of("0"))),
            DatasetMetadata.MATCH_ALL_SERVICE);
    BiPredicate<String, List<Trace.Span>> predicate =
        rateLimiter.createBulkIngestRateLimiter(List.of(datasetMetadata));

    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isFalse();

    Thread.sleep(2000);

    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isFalse();
  }

  @Test
  public void shouldAllowBurstingOverLimitCold() throws InterruptedException {
    String name = "rateLimiter";
    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey(SERVICE_NAME_KEY).setVStr(name).build())
            .build();

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    PreprocessorRateLimiter rateLimiter = new PreprocessorRateLimiter(meterRegistry, 1, 2, false);

    long targetThroughput = span.getSerializedSize() - 1;
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            name,
            name,
            targetThroughput,
            List.of(new DatasetPartitionMetadata(100, Long.MAX_VALUE, List.of("0"))),
            DatasetMetadata.MATCH_ALL_SERVICE);
    BiPredicate<String, List<Trace.Span>> predicate =
        rateLimiter.createBulkIngestRateLimiter(List.of(datasetMetadata));

    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isFalse();

    Thread.sleep(2500);

    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isTrue();
    assertThat(predicate.test("key", List.of(span))).isFalse();
  }
}
