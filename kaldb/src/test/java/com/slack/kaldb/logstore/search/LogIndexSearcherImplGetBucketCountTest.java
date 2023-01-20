package com.slack.kaldb.logstore.search;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class LogIndexSearcherImplGetBucketCountTest {
  @Test
  @Deprecated // see LogIndexSearcherImpl.getBucketCount
  public void getBucketCountShouldCorrectlyConvertInterval() {
    Instant now = Instant.now();

    // 1m interval for 1 day should be 1440
    assertThat(
            LogIndexSearcherImpl.getBucketCount(
                new SearchAggregation(
                    "foo",
                    "date_histogram",
                    Map.of(
                        "interval",
                        "1m",
                        "extended_bounds",
                        Map.of(
                            "min", now.minus(1, ChronoUnit.DAYS).toEpochMilli(),
                            "max", now.toEpochMilli())),
                    List.of())))
        .isEqualTo(1440);

    // 1d interval for 1 day should be 1
    assertThat(
            LogIndexSearcherImpl.getBucketCount(
                new SearchAggregation(
                    "foo",
                    "date_histogram",
                    Map.of(
                        "interval",
                        "1d",
                        "extended_bounds",
                        Map.of(
                            "min", now.minus(1, ChronoUnit.DAYS).toEpochMilli(),
                            "max", now.toEpochMilli())),
                    List.of())))
        .isEqualTo(1);

    // 1s interval for 1 day should be 86400
    assertThat(
            LogIndexSearcherImpl.getBucketCount(
                new SearchAggregation(
                    "foo",
                    "date_histogram",
                    Map.of(
                        "interval",
                        "1s",
                        "extended_bounds",
                        Map.of(
                            "min", now.minus(1, ChronoUnit.DAYS).toEpochMilli(),
                            "max", now.toEpochMilli())),
                    List.of())))
        .isEqualTo(86400);

    // should gracefully handle invalid list of aggregations
    assertThat(
            LogIndexSearcherImpl.getBucketCount(
                new SearchAggregation("foo", "date_histogram", Map.of(), List.of())))
        .isEqualTo(60);

    // should gracefully handle invalid aggregation
    assertThat(
            LogIndexSearcherImpl.getBucketCount(
                new SearchAggregation(
                    "foo", "date_histogram", Map.of("interval", "garbage"), List.of())))
        .isEqualTo(60);

    // should gracefully handle multiple aggregations
    assertThat(
            LogIndexSearcherImpl.getBucketCount(
                new SearchAggregation(
                    "foo",
                    "date_histogram",
                    Map.of(
                        "interval",
                        "1d",
                        "extended_bounds",
                        Map.of(
                            "min", now.minus(1, ChronoUnit.DAYS).toEpochMilli(),
                            "max", now.toEpochMilli())),
                    List.of(
                        new SearchAggregation(
                            "bar",
                            "date_histogram",
                            Map.of(
                                "interval",
                                "1s",
                                "extended_bounds",
                                Map.of(
                                    "min", now.minus(1, ChronoUnit.DAYS).toEpochMilli(),
                                    "max", now.toEpochMilli())),
                            List.of())))))
        .isEqualTo(1);
  }
}
