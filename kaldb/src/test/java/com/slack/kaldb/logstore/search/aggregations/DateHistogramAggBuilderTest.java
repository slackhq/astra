package com.slack.kaldb.logstore.search.aggregations;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.Test;

public class DateHistogramAggBuilderTest {

  @Test
  public void testEqualsAndHashCode() {
    assertThat(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                0,
                "epoch_ms",
                Map.of("max", 1L, "min", 0L),
                List.of(new AvgAggBuilder("name", "field", null))))
        .isEqualTo(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                0,
                "epoch_ms",
                Map.of("max", 1L, "min", 0L),
                List.of(new AvgAggBuilder("name", "field", null))));
    assertThat(
            new DateHistogramAggBuilder(
                    "name",
                    "field",
                    "1d",
                    "10s",
                    0,
                    "epoch_ms",
                    Map.of("max", 1L, "min", 0L),
                    List.of(new AvgAggBuilder("name", "field", null)))
                .hashCode())
        .isEqualTo(
            new DateHistogramAggBuilder(
                    "name",
                    "field",
                    "1d",
                    "10s",
                    0,
                    "epoch_ms",
                    Map.of("max", 1L, "min", 0L),
                    List.of(new AvgAggBuilder("name", "field", null)))
                .hashCode());

    assertThat(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field", null))))
        .isEqualTo(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field", null))));

    assertThat(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "1d",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field", null))))
        .isNotEqualTo(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field", null))));

    assertThat(
            new DateHistogramAggBuilder(
                "name", "field", "12d", "10s", 1, "epoch_ms", Map.of(), List.of()))
        .isNotEqualTo(
            new DateHistogramAggBuilder(
                "name", "field", "1d", "10s", 1, "epoch_ms", Map.of(), List.of()));

    assertThat(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                0,
                "epoch_ms",
                Map.of("max", 1L, "min", 0L),
                List.of(new AvgAggBuilder("name", "field", null))))
        .isNotEqualTo(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                0,
                "epoch_ms",
                Map.of("max", 1L, "min", 1L),
                List.of(new AvgAggBuilder("name", "field", null))));

    assertThat(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field1", null))))
        .isNotEqualTo(
            new DateHistogramAggBuilder(
                "name",
                "field",
                "1d",
                "10s",
                1,
                "epoch_ms",
                Map.of(),
                List.of(new AvgAggBuilder("name", "field2", null))));
  }
}
