package com.slack.kaldb.elasticsearchApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.Resources;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.action.index.IndexRequest;

public class OpenSearchBulkRequestTest {

  private String getRawQueryString(String filename) throws IOException {
    return Resources.toString(
        Resources.getResource(String.format("opensearchRequest/bulk/%s.ndjson", filename)),
        Charset.defaultCharset());
  }

  @Test
  public void testSimpleIndexRequest() throws Exception {
    String rawRequest = getRawQueryString("index_simple");

    List<IndexRequest> indexRequests = OpenSearchRequest.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);
    assertThat(indexRequests.get(0).index()).isEqualTo("test");
    assertThat(indexRequests.get(0).id()).isEqualTo("1");
    assertThat(indexRequests.get(0).sourceAsMap().size()).isEqualTo(2);

    Map<String, List<Trace.Span>> indexDocs =
        OpenSearchRequest.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("test").size()).isEqualTo(1);

    assertThat(indexDocs.get("test").get(0).getId().toStringUtf8()).isEqualTo("1");
    assertThat(indexDocs.get("test").get(0).getTagsList().size()).isEqualTo(3);
    assertThat(
            indexDocs.get("test").get(0).getTagsList().stream()
                .filter(
                    keyValue ->
                        keyValue.getKey().equals("service_name")
                            && keyValue.getVStr().equals("test"))
                .count())
        .isEqualTo(1);
  }

  @Test
  public void testOtherBulkRequests() throws Exception {
    String rawRequest = getRawQueryString("non_index");
    List<IndexRequest> indexRequests = OpenSearchRequest.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(0);
  }

  @Test
  public void testBulkRequests() throws Exception {
    String rawRequest = getRawQueryString("bulk_requests");
    List<IndexRequest> indexRequests = OpenSearchRequest.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(0);
  }
}
