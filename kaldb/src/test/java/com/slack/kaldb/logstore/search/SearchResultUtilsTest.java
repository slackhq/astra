package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.logstore.search.SearchResultUtils.fromValueProto;
import static com.slack.kaldb.logstore.search.SearchResultUtils.toValueProto;
import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.HistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.MinAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.PercentilesAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.TermsAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.UniqueCountAggBuilder;
import com.slack.kaldb.proto.service.KaldbSearch;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SearchResultUtilsTest {

  @Test
  public void shouldConvertMinAggToFromProto() {
    MinAggBuilder minAggBuilder =
        new MinAggBuilder("1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "3", "return 8;");

    KaldbSearch.SearchRequest.SearchAggregation searchAggregation =
        SearchResultUtils.toSearchAggregationProto(minAggBuilder);
    MinAggBuilder otherMinAggBuilder =
        (MinAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);

    assertThat(minAggBuilder).isEqualTo(otherMinAggBuilder);
  }

  @Test
  public void shouldConvertAvgAggToFromProto() {
    AvgAggBuilder avgAggBuilder1 =
        new AvgAggBuilder("1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "3", "return 9;");

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
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
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
  public void shouldConvertHistogramAggToFromProto() {
    HistogramAggBuilder histogramAggBuilder1 =
        new HistogramAggBuilder("1", "@timestamp", "1000", 1, List.of());

    KaldbSearch.SearchRequest.SearchAggregation searchAggregation =
        SearchResultUtils.toSearchAggregationProto(histogramAggBuilder1);
    HistogramAggBuilder histogramAggBuilder2 =
        (HistogramAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);

    assertThat(histogramAggBuilder1).isEqualTo(histogramAggBuilder2);
  }

  @Test
  public void shouldConvertTermsAggToFromProto() {
    TermsAggBuilder termsAggBuilder1 =
        new TermsAggBuilder(
            "foo",
            List.of(),
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            3,
            100,
            0,
            Map.of("_term", "asc"));

    KaldbSearch.SearchRequest.SearchAggregation searchAggregation =
        SearchResultUtils.toSearchAggregationProto(termsAggBuilder1);
    TermsAggBuilder termsAggBuilder2 =
        (TermsAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);

    assertThat(termsAggBuilder1).isEqualTo(termsAggBuilder2);
  }

  @Test
  public void shouldConvertUniqueCountToFromProto() {
    UniqueCountAggBuilder uniqueCountAggBuilder1 =
        new UniqueCountAggBuilder("foo", "service_name", "2", 1L);

    KaldbSearch.SearchRequest.SearchAggregation searchAggregation =
        SearchResultUtils.toSearchAggregationProto(uniqueCountAggBuilder1);
    UniqueCountAggBuilder uniqueCountAggBuilder2 =
        (UniqueCountAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);

    assertThat(uniqueCountAggBuilder1).isEqualTo(uniqueCountAggBuilder2);
  }

  @Test
  public void shouldConvertPercentilesToFromProto() {
    PercentilesAggBuilder percentilesAggBuilder1 =
        new PercentilesAggBuilder("foo", "service_name", "2", List.of(99D, 100D), "return 8;");

    KaldbSearch.SearchRequest.SearchAggregation searchAggregation =
        SearchResultUtils.toSearchAggregationProto(percentilesAggBuilder1);
    PercentilesAggBuilder percentilesAggBuilder2 =
        (PercentilesAggBuilder) SearchResultUtils.fromSearchAggregations(searchAggregation);

    assertThat(percentilesAggBuilder1).isEqualTo(percentilesAggBuilder2);
  }

  @Test
  public void shouldConvertNestedAggregations() {
    // this is not representative of a real or reasonable query, but we should be able to convert it
    // just the same
    AvgAggBuilder avgAggBuilder =
        new AvgAggBuilder("1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, null, null);

    DateHistogramAggBuilder dateHistogramAggBuilderInner =
        new DateHistogramAggBuilder(
            "1",
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
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

  @Test
  public void shouldConvertValueToFromProto() {
    assertThat(fromValueProto(toValueProto(null))).isEqualTo(null);
    assertThat(fromValueProto(toValueProto(1))).isEqualTo(1);
    assertThat(fromValueProto(toValueProto(2L))).isEqualTo(2L);
    assertThat(fromValueProto(toValueProto(3D))).isEqualTo(3D);
    assertThat(fromValueProto(toValueProto("4"))).isEqualTo("4");
    assertThat(fromValueProto(toValueProto(false))).isEqualTo(false);
    assertThat(fromValueProto(toValueProto(Map.of("1", 2, "3", false))))
        .isEqualTo(Map.of("1", 2, "3", false));
    assertThat(fromValueProto(toValueProto(List.of("1", 2, 3D, false))))
        .isEqualTo(List.of("1", 2, 3D, false));
  }
}
