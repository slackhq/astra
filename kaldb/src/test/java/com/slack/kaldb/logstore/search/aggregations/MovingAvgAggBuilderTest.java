package com.slack.kaldb.logstore.search.aggregations;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class MovingAvgAggBuilderTest {

  @Test
  public void testEqualsAndHashCode() {

    assertThat(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false))
        .isEqualTo(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false));
    assertThat(
            new MovingAvgAggBuilder(
                    "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false)
                .hashCode())
        .isEqualTo(
            new MovingAvgAggBuilder(
                    "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false)
                .hashCode());

    assertThat(new MovingAvgAggBuilder("name", "_count", "linear", 5, 2))
        .isEqualTo(new MovingAvgAggBuilder("name", "_count", "linear", 5, 2));

    assertThat(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false))
        .isNotEqualTo(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, true));
    assertThat(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false))
        .isNotEqualTo(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 3, true, false));
    assertThat(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false))
        .isNotEqualTo(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.2, 4, true, false));
    assertThat(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false))
        .isNotEqualTo(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.1, 0.3, 4, true, false));
    assertThat(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false))
        .isNotEqualTo(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.11, 0.2, 0.3, 4, true, false));
    assertThat(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false))
        .isNotEqualTo(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 1, 0.1, 0.2, 0.3, 4, true, false));
    assertThat(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false))
        .isNotEqualTo(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 4, 2, 0.1, 0.2, 0.3, 4, true, false));
    assertThat(
            new MovingAvgAggBuilder(
                "name", "_count", "holt_winters", 5, 2, 0.1, 0.2, 0.3, 4, true, false))
        .isNotEqualTo(
            new MovingAvgAggBuilder("name", "_count", "holt", 5, 2, 0.1, 0.2, 0.3, 4, true, false));
  }
}
