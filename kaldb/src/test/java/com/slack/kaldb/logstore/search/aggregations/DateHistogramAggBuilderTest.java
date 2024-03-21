package com.slack.kaldb.logstore.search.aggregations;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class DateHistogramAggBuilderTest {

  @Test
  public void testEqualsAndHashCode() {
    assertThat(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                "-01:00",
                0,
                "epoch_ms",
                Map.of("max", 1L, "min", 0L),
                List.of(new AvgAggBuilder("name", "field", null, null))))
        .isEqualTo(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                "-01:00",
                0,
                "epoch_ms",
                Map.of("max", 1L, "min", 0L),
                List.of(new AvgAggBuilder("name", "field", null, null))));
    assertThat(
            new DateHistogramAggBuilder(
                    "name",
                    "field",
                    "1d",
                    "10s",
                    "-01:00",
                    0,
                    "epoch_ms",
                    Map.of("max", 1L, "min", 0L),
                    List.of(new AvgAggBuilder("name", "field", null, null)))
                .hashCode())
        .isEqualTo(
            new DateHistogramAggBuilder(
                    "name",
                    "field",
                    "1d",
                    "10s",
                    "-01:00",
                    0,
                    "epoch_ms",
                    Map.of("max", 1L, "min", 0L),
                    List.of(new AvgAggBuilder("name", "field", null, null)))
                .hashCode());

    assertThat(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                "-01:00",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field", null, null))))
        .isEqualTo(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                "-01:00",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field", null, null))));

    assertThat(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "1d",
                "-01:00",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field", null, null))))
        .isNotEqualTo(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                "-01:00",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field", null, null))));

    assertThat(
            new DateHistogramAggBuilder(
                "name", "field", "12d", "10s", "-01:00", 1, "epoch_ms", Map.of(), List.of()))
        .isNotEqualTo(
            new DateHistogramAggBuilder(
                "name", "field", "1d", "10s", "-01:00", 1, "epoch_ms", Map.of(), List.of()));

    assertThat(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                "-01:00",
                0,
                "epoch_ms",
                Map.of("max", 1L, "min", 0L),
                List.of(new AvgAggBuilder("name", "field", null, null))))
        .isNotEqualTo(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                "-01:00",
                0,
                "epoch_ms",
                Map.of("max", 1L, "min", 1L),
                List.of(new AvgAggBuilder("name", "field", null, null))));

    assertThat(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                "-01:00",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field1", null, null))))
        .isNotEqualTo(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                "-01:00",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field2", null, null))));

    assertThat(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                null,
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field1", null, null))))
        .isNotEqualTo(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                "-01:00",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field1", null, null))));
  }
}
