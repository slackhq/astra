package com.slack.kaldb.logstore.search.aggregations;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class AvgAggBuilderTest {

  @Test
  public void testEqualsAndHashCode() {

    assertThat(new AvgAggBuilder("name", "field", null))
        .isEqualTo(new AvgAggBuilder("name", "field", null));
    assertThat(new AvgAggBuilder("name", "field", null).hashCode())
        .isEqualTo(new AvgAggBuilder("name", "field", null).hashCode());
    assertThat(new AvgAggBuilder("name", "field", 2L))
        .isEqualTo(new AvgAggBuilder("name", "field", 2L));

    assertThat(new AvgAggBuilder("name", "field", null))
        .isNotEqualTo(new AvgAggBuilder("name", "field", 2L));
    assertThat(new AvgAggBuilder("name", "field1", 2L))
        .isNotEqualTo(new AvgAggBuilder("name", "field", 2L));
    assertThat(new AvgAggBuilder("name1", "field", null))
        .isNotEqualTo(new AvgAggBuilder("name", "field", null));
  }
}
