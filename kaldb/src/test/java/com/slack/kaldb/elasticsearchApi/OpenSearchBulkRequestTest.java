package com.slack.kaldb.elasticsearchApi;

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.Charset;
import org.junit.jupiter.api.Test;

public class OpenSearchBulkRequestTest {

  private String getRawQueryString(String filename) throws IOException {
    return Resources.toString(
        Resources.getResource(String.format("opensearchRequest/bulk/%s.ndjson", filename)),
        Charset.defaultCharset());
  }

  @Test
  public void testBasicIndexRequest() throws Exception {
    String rawRequest = getRawQueryString("example");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    openSearchRequest.parseBulkHttpRequest(rawRequest);
  }
}
