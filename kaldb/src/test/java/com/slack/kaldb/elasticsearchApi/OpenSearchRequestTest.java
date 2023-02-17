package com.slack.kaldb.elasticsearchApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.Resources;
import com.slack.kaldb.proto.service.KaldbSearch;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import org.junit.Test;

public class OpenSearchRequestTest {

  private String getRawQueryString(String filename) throws IOException {
    return Resources.toString(
        Resources.getResource(String.format("opensearchRequest/%s.ndjson", filename)),
        Charset.defaultCharset());
  }

  @Test
  public void testGeneralFields() throws Exception {
    // todo - time range, query string, dataset
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
    // todo - add other asserts
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
    assertThat(dateHistogramAggBuilder.getName()).isEqualTo("2");
    // todo - add other asserts

    KaldbSearch.SearchRequest.SearchAggregation avgAggBuilder =
        parsedRequestList.get(0).getAggregations().getSubAggregations(0);
    assertThat(avgAggBuilder.getName()).isEqualTo("3");
    assertThat(avgAggBuilder.getValueSource().getField()).isEqualTo("duration_ms");
  }
}
