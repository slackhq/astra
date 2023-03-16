package com.slack.kaldb.elasticsearchApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.Resources;
import com.slack.kaldb.proto.service.KaldbSearch;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class OpenSearchRequestTest {

  private String getRawQueryString(String filename) throws IOException {
    return Resources.toString(
        Resources.getResource(String.format("opensearchRequest/%s.ndjson", filename)),
        Charset.defaultCharset());
  }

  @Test
  public void testGeneralFields() throws Exception {
    String rawRequest = getRawQueryString("datehistogram");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<KaldbSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    KaldbSearch.SearchRequest request = parsedRequestList.get(0);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(1);
    assertThat(request.getQueryString()).isEqualTo("*:*");
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1676498801027L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1676500240688L);
  }

  @Test
  public void testDateHistogram() throws Exception {
    String rawRequest = getRawQueryString("datehistogram");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<KaldbSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    KaldbSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();

    assertThat(dateHistogramAggBuilder.getName()).isEqualTo("2");
    assertThat(dateHistogramAggBuilder.getValueSource().getField()).isEqualTo("@timestamp");
    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(0);

    KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.DateHistogramAggregation
        dateHistogramAggregation = dateHistogramAggBuilder.getValueSource().getDateHistogram();
    assertThat(dateHistogramAggregation.getInterval()).isEqualTo("10s");
    assertThat(dateHistogramAggregation.getMinDocCount()).isEqualTo(90000);
    assertThat(dateHistogramAggregation.getExtendedBoundsMap())
        .isEqualTo(
            Map.of(
                "min", 1676498801027L,
                "max", 1676500240688L));
    assertThat(dateHistogramAggregation.getFormat()).isEqualTo("epoch_millis");
    assertThat(dateHistogramAggregation.getOffset()).isEqualTo("5s");
  }

  @Test
  public void testHistogram() throws Exception {
    String rawRequest = getRawQueryString("histogram");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<KaldbSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    KaldbSearch.SearchRequest.SearchAggregation histogramAggBuilder =
        parsedRequestList.get(0).getAggregations();

    assertThat(histogramAggBuilder.getName()).isEqualTo("2");
    assertThat(histogramAggBuilder.getValueSource().getField()).isEqualTo("@timestamp");
    assertThat(histogramAggBuilder.getSubAggregationsCount()).isEqualTo(0);

    KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.HistogramAggregation
        histogramAggregation = histogramAggBuilder.getValueSource().getHistogram();
    assertThat(histogramAggregation.getInterval()).isEqualTo("1000");
    assertThat(histogramAggregation.getMinDocCount()).isEqualTo(1);
  }

  @Test
  public void testUniqueCount() throws Exception {
    String rawRequest = getRawQueryString("unique_count");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<KaldbSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    KaldbSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);

    KaldbSearch.SearchRequest.SearchAggregation uniqueCountAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);

    assertThat(uniqueCountAggBuilder.getType()).isEqualTo("cardinality");
    assertThat(uniqueCountAggBuilder.getName()).isEqualTo("1");
    assertThat(uniqueCountAggBuilder.getValueSource().getField()).isEqualTo("service_name");
    assertThat(uniqueCountAggBuilder.getValueSource().getMissing().hasNullValue()).isTrue();
    assertThat(
            uniqueCountAggBuilder
                .getValueSource()
                .getUniqueCount()
                .getPrecisionThreshold()
                .getLongValue())
        .isEqualTo(1);
  }

  @Test
  public void testPercentiles() throws Exception {
    String rawRequest = getRawQueryString("percentiles");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<KaldbSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    KaldbSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);

    KaldbSearch.SearchRequest.SearchAggregation percentileAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);
    assertThat(percentileAggBuilder.getType()).isEqualTo("percentiles");
    assertThat(percentileAggBuilder.getName()).isEqualTo("5");
    assertThat(percentileAggBuilder.getValueSource().getField()).isEqualTo("service_name");
    assertThat(percentileAggBuilder.getValueSource().getMissing().hasNullValue()).isTrue();
    assertThat(percentileAggBuilder.getValueSource().getPercentiles().getPercentilesList())
        .containsExactly(25D, 50D, 75D, 95D, 99D);
  }

  @Test
  public void testHistogramWithNestedAvg() throws Exception {
    String rawRequest = getRawQueryString("nested_datehistogram_avg");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<KaldbSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    KaldbSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getValueSource().getField()).isEqualTo("@timestamp");

    KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.DateHistogramAggregation
        dateHistogramAggregation = dateHistogramAggBuilder.getValueSource().getDateHistogram();
    assertThat(dateHistogramAggregation.getInterval()).isEqualTo("10s");
    assertThat(dateHistogramAggregation.getMinDocCount()).isEqualTo(90000);
    assertThat(dateHistogramAggregation.getExtendedBoundsMap())
        .isEqualTo(
            Map.of(
                "min", 1676498801027L,
                "max", 1676500240688L));
    assertThat(dateHistogramAggregation.getFormat()).isEqualTo("epoch_millis");
    assertThat(dateHistogramAggregation.getOffset()).isEqualTo("5s");

    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);
    KaldbSearch.SearchRequest.SearchAggregation avgAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);
    assertThat(avgAggBuilder.getName()).isEqualTo("3");
    assertThat(avgAggBuilder.getValueSource().getField()).isEqualTo("duration_ms");
  }

  @Test
  public void testDateHistogramWithNestedMovingAvg() throws IOException {
    String rawRequest = getRawQueryString("datehistogram_movavg");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<KaldbSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseHttpPostBody(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    KaldbSearch.SearchRequest.SearchAggregation dateHistogramAggBuilder =
        parsedRequestList.get(0).getAggregations();
    assertThat(dateHistogramAggBuilder.getValueSource().getField()).isEqualTo("@timestamp");

    // todo - more asserts
    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);
    assertThat(dateHistogramAggBuilder.getSubAggregationsCount()).isEqualTo(1);
    KaldbSearch.SearchRequest.SearchAggregation movAvgAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);
    assertThat(movAvgAggBuilder.getName()).isEqualTo("3");
    assertThat(movAvgAggBuilder.getType()).isEqualTo("moving_avg");
    assertThat(movAvgAggBuilder.getPipeline().getBucketsPath()).isEqualTo("_count");
  }
}
