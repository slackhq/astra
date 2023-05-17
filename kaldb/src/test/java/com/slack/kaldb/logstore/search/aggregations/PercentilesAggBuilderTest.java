package com.slack.kaldb.logstore.search.aggregations;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

public class PercentilesAggBuilderTest {

  @Test
  public void testEqualsAndHashCode() {
    assertThat(new PercentilesAggBuilder("name", "field", 2L, List.of(99D), null))
        .isEqualTo(new PercentilesAggBuilder("name", "field", 2L, List.of(99D), null));

    assertThat(new PercentilesAggBuilder("name", "field", null, List.of(99D), null))
        .isEqualTo(new PercentilesAggBuilder("name", "field", null, List.of(99D), null));

    assertThat(new PercentilesAggBuilder("name", "field", null, List.of(99D, 98D), null))
        .isEqualTo(new PercentilesAggBuilder("name", "field", null, List.of(99D, 98D), null));

    assertThat(new PercentilesAggBuilder("name", "field", null, List.of(99D, 98D), null).hashCode())
        .isEqualTo(
            new PercentilesAggBuilder("name", "field", null, List.of(99D, 98D), null).hashCode());

    assertThat(new PercentilesAggBuilder("name", "field", null, List.of(99D, 98D), null))
        .isNotEqualTo(new PercentilesAggBuilder("name", "field", null, List.of(99D), null));

    assertThat(new PercentilesAggBuilder("name1", "field", null, List.of(), null))
        .isNotEqualTo(new PercentilesAggBuilder("name2", "field", null, List.of(), null));

    assertThat(new PercentilesAggBuilder("name", "field1", null, List.of(), null))
        .isNotEqualTo(new PercentilesAggBuilder("name", "field2", null, List.of(), null));
    assertThat(new PercentilesAggBuilder("name", "field", null, List.of(), ""))
        .isNotEqualTo(new PercentilesAggBuilder("name", "field", null, List.of(), null));
  }
}
