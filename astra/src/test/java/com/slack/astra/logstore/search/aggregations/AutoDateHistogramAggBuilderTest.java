package com.slack.astra.logstore.search.aggregations;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

public class AutoDateHistogramAggBuilderTest {

  @Test
  public void testEqualsAndHashCode() {
    assertThat(
            new AutoDateHistogramAggBuilder(
                "name",
                "field",
                "day",
                10,
                List.of(new AvgAggBuilder("name", "field", null, null))))
        .isEqualTo(
            new AutoDateHistogramAggBuilder(
                "name",
                "field",
                "day",
                10,
                List.of(new AvgAggBuilder("name", "field", null, null))));

    assertThat(
            new AutoDateHistogramAggBuilder(
                "name",
                "field",
                null,
                null,
                List.of(new AvgAggBuilder("name", "field", null, null))))
        .isEqualTo(
            new AutoDateHistogramAggBuilder(
                "name",
                "field",
                null,
                null,
                List.of(new AvgAggBuilder("name", "field", null, null))));

    assertThat(
            new AutoDateHistogramAggBuilder(
                "name",
                "field",
                "day",
                null,
                List.of(new AvgAggBuilder("name", "field", null, null))))
        .isNotEqualTo(
            new AutoDateHistogramAggBuilder(
                "name",
                "field",
                null,
                null,
                List.of(new AvgAggBuilder("name", "field", null, null))));

    assertThat(
            new AutoDateHistogramAggBuilder(
                "name", "field", null, 10, List.of(new AvgAggBuilder("name", "field", null, null))))
        .isNotEqualTo(
            new AutoDateHistogramAggBuilder(
                "name",
                "field",
                null,
                null,
                List.of(new AvgAggBuilder("name", "field", null, null))));
  }
}
