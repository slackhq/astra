package com.slack.astra.logstore.search.aggregations;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class AvgAggBuilderTest {

  @Test
  public void testEqualsAndHashCode() {

    assertThat(new AvgAggBuilder("name", "field", null, null))
        .isEqualTo(new AvgAggBuilder("name", "field", null, null));
    assertThat(new AvgAggBuilder("name", "field", null, null).hashCode())
        .isEqualTo(new AvgAggBuilder("name", "field", null, null).hashCode());
    assertThat(new AvgAggBuilder("name", "field", 2L, null))
        .isEqualTo(new AvgAggBuilder("name", "field", 2L, null));
    assertThat(new AvgAggBuilder("name", "field", 2L, "return 9;"))
        .isEqualTo(new AvgAggBuilder("name", "field", 2L, "return 9;"));

    assertThat(new AvgAggBuilder("name", "field", null, null))
        .isNotEqualTo(new AvgAggBuilder("name", "field", 2L, null));
    assertThat(new AvgAggBuilder("name", "field1", 2L, null))
        .isNotEqualTo(new AvgAggBuilder("name", "field", 2L, null));
    assertThat(new AvgAggBuilder("name1", "field", null, null))
        .isNotEqualTo(new AvgAggBuilder("name", "field", null, null));
    assertThat(new AvgAggBuilder("name", "field", null, ""))
        .isNotEqualTo(new AvgAggBuilder("name", "field", null, null));
  }
}
