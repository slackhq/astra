package com.slack.kaldb.logstore.search.aggregations;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class UniqueCountAggBuilderTest {

  @Test
  public void testEqualsAndHashCode() {
    assertThat(new UniqueCountAggBuilder("name", "field", null, 1L))
        .isEqualTo(new UniqueCountAggBuilder("name", "field", null, 1L));

    assertThat(new UniqueCountAggBuilder("name", "field", 2L, 1L))
        .isEqualTo(new UniqueCountAggBuilder("name", "field", 2L, 1L));

    assertThat(new UniqueCountAggBuilder("name", "field", null, 1L).hashCode())
        .isEqualTo(new UniqueCountAggBuilder("name", "field", null, 1L).hashCode());

    assertThat(new UniqueCountAggBuilder("name1", "field", null, 1L).hashCode())
        .isNotEqualTo(new UniqueCountAggBuilder("name2", "field", null, 1L).hashCode());

    assertThat(new UniqueCountAggBuilder("name", "field", null, 1L).hashCode())
        .isNotEqualTo(new UniqueCountAggBuilder("name", "field", 2L, 1L).hashCode());

    assertThat(new UniqueCountAggBuilder("name", "field", null, 1L).hashCode())
        .isNotEqualTo(new UniqueCountAggBuilder("name", "field", null, 2L).hashCode());
  }
}
