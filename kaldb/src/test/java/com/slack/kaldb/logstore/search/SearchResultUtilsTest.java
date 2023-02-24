package com.slack.kaldb.logstore.search;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.proto.service.KaldbSearch;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SearchResultUtilsTest {

  @Test
  public void shouldConvertAvgAggToFromProto() {
    AvgAggBuilder avgAggBuilder1 = new AvgAggBuilder("1", "@timestamp");

    KaldbSearch.SearchRequest.SearchAggregation searchAggregation =
        SearchResultUtils.toSearchAggregationProto(avgAggBuilder1);
    AvgAggBuilder avgAggBuilder2 =
        (AvgAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);

    assertThat(avgAggBuilder1).isEqualTo(avgAggBuilder2);
  }

  @Test
  public void shouldConvertDateHistogramAggToFromProto() {
    DateHistogramAggBuilder dateHistogramAggBuilder1 =
        new DateHistogramAggBuilder(
            "1",
            "@timestamp",
            "5s",
            "2s",
            10000,
            "epoch_ms",
            Map.of(
                "min", Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                "max", Instant.now().toEpochMilli()),
            List.of());

    KaldbSearch.SearchRequest.SearchAggregation searchAggregation =
        SearchResultUtils.toSearchAggregationProto(dateHistogramAggBuilder1);
    DateHistogramAggBuilder dateHistogramAggBuilder2 =
        (DateHistogramAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);

    assertThat(dateHistogramAggBuilder1).isEqualTo(dateHistogramAggBuilder2);
  }

  @Test
  public void shouldConvertNestedAggregations() {
    // this is not representative of a real or reasonable query, but we should be able to convert it
    // just the same
    AvgAggBuilder avgAggBuilder = new AvgAggBuilder("1", "@timestamp");

    DateHistogramAggBuilder dateHistogramAggBuilderInner =
        new DateHistogramAggBuilder(
            "1",
            "@timestamp",
            "5s",
            "2s",
            10000,
            "epoch_ms",
            Map.of(
                "min", Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                "max", Instant.now().toEpochMilli()),
            List.of(avgAggBuilder));

    DateHistogramAggBuilder dateHistogramAggBuilder1 =
        new DateHistogramAggBuilder(
            "2",
            "duration_ms",
            "10s",
            "7s",
            1000,
            "epoch_ms",
            Map.of(
                "min", Instant.now().minus(2, ChronoUnit.MINUTES).toEpochMilli(),
                "max", Instant.now().plus(2, ChronoUnit.MINUTES).toEpochMilli()),
            List.of(dateHistogramAggBuilderInner));

    KaldbSearch.SearchRequest.SearchAggregation searchAggregation =
        SearchResultUtils.toSearchAggregationProto(dateHistogramAggBuilder1);
    DateHistogramAggBuilder dateHistogramAggBuilder2 =
        (DateHistogramAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);

    assertThat(dateHistogramAggBuilder1).isEqualTo(dateHistogramAggBuilder2);
    assertThat(dateHistogramAggBuilder1.getSubAggregations())
        .isEqualTo(dateHistogramAggBuilder2.getSubAggregations());
  }
}
