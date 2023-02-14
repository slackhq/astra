package com.slack.kaldb.elasticsearchApi.searchRequest;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.slack.kaldb.elasticsearchApi.searchRequest.aggregations.DateHistogramAggregation;
import com.slack.kaldb.logstore.LogMessage;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class EsSearchRequestTest {

  @Test
  @Deprecated // see EsSearchRequest.getBucketCount
  public void getBucketCountShouldCorrectlyConvertInterval() {
    Instant now = Instant.now();

    // 1m interval for 1 day should be 1440
    assertThat(
            EsSearchRequest.getBucketCount(
                List.of(
                    new DateHistogramAggregation(
                        "foo",
                        "1m",
                        100,
                        "",
                        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                        "",
                        Map.of())),
                new SearchRequestTimeRange(
                    now.minus(1, ChronoUnit.DAYS).toEpochMilli(), now.toEpochMilli())))
        .isEqualTo(1440);

    // 1d interval for 1 day should be 1
    assertThat(
            EsSearchRequest.getBucketCount(
                List.of(
                    new DateHistogramAggregation(
                        "foo",
                        "1d",
                        100,
                        "",
                        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                        "",
                        Map.of())),
                new SearchRequestTimeRange(
                    now.minus(1, ChronoUnit.DAYS).toEpochMilli(), now.toEpochMilli())))
        .isEqualTo(1);

    // 1s interval for 1 day should be 86400
    assertThat(
            EsSearchRequest.getBucketCount(
                List.of(
                    new DateHistogramAggregation(
                        "foo",
                        "1s",
                        100,
                        "",
                        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                        "",
                        Map.of())),
                new SearchRequestTimeRange(
                    now.minus(1, ChronoUnit.DAYS).toEpochMilli(), now.toEpochMilli())))
        .isEqualTo(86400);

    // should gracefully handle invalid list of aggregations
    assertThat(
            EsSearchRequest.getBucketCount(
                List.of(),
                new SearchRequestTimeRange(
                    now.minus(1, ChronoUnit.DAYS).toEpochMilli(), now.toEpochMilli())))
        .isEqualTo(60);

    // should gracefully handle invalid aggregations
    assertThat(
            EsSearchRequest.getBucketCount(
                List.of(
                    new DateHistogramAggregation(
                        "foo",
                        "garbage",
                        100,
                        "",
                        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                        "",
                        Map.of())),
                new SearchRequestTimeRange(
                    now.minus(1, ChronoUnit.DAYS).toEpochMilli(), now.toEpochMilli())))
        .isEqualTo(60);

    // should gracefully handle multiple aggregations
    assertThat(
            EsSearchRequest.getBucketCount(
                List.of(
                    new DateHistogramAggregation(
                        "foo",
                        "1d",
                        100,
                        "",
                        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                        "",
                        Map.of()),
                    new DateHistogramAggregation(
                        "foo",
                        "1s",
                        100,
                        "",
                        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                        "",
                        Map.of())),
                new SearchRequestTimeRange(
                    now.minus(1, ChronoUnit.DAYS).toEpochMilli(), now.toEpochMilli())))
        .isEqualTo(1);
  }
}
