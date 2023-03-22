package com.slack.kaldb.logstore.search.aggregations;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.Test;

public class PercentilesAggBuilderTest {

  @Test
  public void testEqualsAndHashCode() {
    assertThat(new PercentilesAggBuilder("name", "field", 2L, List.of(99D)))
        .isEqualTo(new PercentilesAggBuilder("name", "field", 2L, List.of(99D)));

    assertThat(new PercentilesAggBuilder("name", "field", null, List.of(99D)))
        .isEqualTo(new PercentilesAggBuilder("name", "field", null, List.of(99D)));

    assertThat(new PercentilesAggBuilder("name", "field", null, List.of(99D, 98D)))
        .isEqualTo(new PercentilesAggBuilder("name", "field", null, List.of(99D, 98D)));

    assertThat(new PercentilesAggBuilder("name", "field", null, List.of(99D, 98D)).hashCode())
        .isEqualTo(new PercentilesAggBuilder("name", "field", null, List.of(99D, 98D)).hashCode());

    assertThat(new PercentilesAggBuilder("name", "field", null, List.of(99D, 98D)))
        .isNotEqualTo(new PercentilesAggBuilder("name", "field", null, List.of(99D)));

    assertThat(new PercentilesAggBuilder("name1", "field", null, List.of()))
        .isNotEqualTo(new PercentilesAggBuilder("name2", "field", null, List.of()));

    assertThat(new PercentilesAggBuilder("name", "field1", null, List.of()))
        .isNotEqualTo(new PercentilesAggBuilder("name", "field2", null, List.of()));
  }
}
