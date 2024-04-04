package com.slack.astra.logstore.search.aggregations;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TermsAggBuilderTest {

  @Test
  public void testEqualsAndHashCode() {
    assertThat(
            new TermsAggBuilder("name", List.of(), "field", null, 10, 0, Map.of("_term", "desc")))
        .isEqualTo(
            new TermsAggBuilder("name", List.of(), "field", null, 10, 0, Map.of("_term", "desc")));

    assertThat(
            new TermsAggBuilder("name", List.of(), "field", null, 10, 0, Map.of("_term", "desc"))
                .hashCode())
        .isEqualTo(
            new TermsAggBuilder("name", List.of(), "field", null, 10, 0, Map.of("_term", "desc"))
                .hashCode());

    assertThat(
            new TermsAggBuilder(
                "name",
                List.of(new AvgAggBuilder("name", "field", null, null)),
                "field",
                1L,
                10,
                1,
                Map.of("_term", "desc")))
        .isEqualTo(
            new TermsAggBuilder(
                "name",
                List.of(new AvgAggBuilder("name", "field", null, null)),
                "field",
                1L,
                10,
                1,
                Map.of("_term", "desc")));

    assertThat(
            new TermsAggBuilder(
                "name",
                List.of(new AvgAggBuilder("name", "field", null, null)),
                "field",
                1L,
                10,
                1,
                Map.of("_term", "desc")))
        .isNotEqualTo(
            new TermsAggBuilder(
                "name",
                List.of(new AvgAggBuilder("name", "field1", null, null)),
                "field",
                1L,
                10,
                1,
                Map.of("_term", "desc")));

    assertThat(
            new TermsAggBuilder(
                "name",
                List.of(new AvgAggBuilder("name", "field", null, null)),
                "field",
                1L,
                10,
                1,
                Map.of("_term", "desc")))
        .isNotEqualTo(
            new TermsAggBuilder("name", List.of(), "field", 1L, 10, 1, Map.of("_term", "desc")));

    assertThat(new TermsAggBuilder("name", List.of(), "field", 1L, 10, 1, Map.of()))
        .isNotEqualTo(new TermsAggBuilder("name", List.of(), "field", null, 10, 1, Map.of()));
  }
}
