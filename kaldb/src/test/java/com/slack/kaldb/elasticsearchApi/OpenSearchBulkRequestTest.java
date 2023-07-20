package com.slack.kaldb.elasticsearchApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.Resources;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class OpenSearchBulkRequestTest {

  private String getRawQueryString(String filename) throws IOException {
    return Resources.toString(
        Resources.getResource(String.format("opensearchRequest/bulk/%s.ndjson", filename)),
        Charset.defaultCharset());
  }

  @Test
  public void testSimpleIndexRequest() throws Exception {
    String rawRequest = getRawQueryString("index_simple");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    Map<String, List<Trace.Span>> docs = openSearchRequest.parseBulkHttpRequest(rawRequest);
    assertThat(docs.size()).isEqualTo(1);
    assertThat(docs.get("test").get(0).getTagsList().size()).isEqualTo(3);
    //
    // assertThat(docs.get("test").get(0).getTagsList().contains("service_name")).isEqualTo(true);
    assertThat(docs.get("test").get(0).getTimestamp()).isGreaterThan(0);
  }
}
