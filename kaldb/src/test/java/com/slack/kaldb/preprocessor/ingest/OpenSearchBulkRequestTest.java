package com.slack.kaldb.preprocessor.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.Resources;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ingest.IngestDocument;

public class OpenSearchBulkRequestTest {

  private String getRawQueryString(String filename) throws IOException {
    return Resources.toString(
        Resources.getResource(String.format("opensearchRequest/bulk/%s.ndjson", filename)),
        Charset.defaultCharset());
  }

  @Test
  public void testSimpleIndexRequest() throws Exception {
    String rawRequest = getRawQueryString("index_simple");

    List<IndexRequest> indexRequests = OpenSearchBulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);
    assertThat(indexRequests.get(0).index()).isEqualTo("test");
    assertThat(indexRequests.get(0).id()).isEqualTo("1");
    assertThat(indexRequests.get(0).sourceAsMap().size()).isEqualTo(2);

    Map<String, List<Trace.Span>> indexDocs =
        OpenSearchBulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
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
    List<IndexRequest> indexRequests = OpenSearchBulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(0);
  }

  @Test
  public void testBulkRequests() throws Exception {
    String rawRequest = getRawQueryString("bulk_requests");
    List<IndexRequest> indexRequests = OpenSearchBulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);

    Map<String, List<Trace.Span>> indexDocs =
        OpenSearchBulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("test").size()).isEqualTo(1);

    assertThat(indexDocs.get("test").get(0).getId().toStringUtf8()).isEqualTo("1");
    assertThat(indexDocs.get("test").get(0).getTagsList().size()).isEqualTo(2);
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
  public void testUpdatesAgainstTwoIndexes() throws Exception {
    String rawRequest = getRawQueryString("two_indexes");
    List<IndexRequest> indexRequests = OpenSearchBulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);

    Map<String, List<Trace.Span>> indexDocs =
        OpenSearchBulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(2);
    assertThat(indexDocs.get("test1").size()).isEqualTo(1);
    assertThat(indexDocs.get("test2").size()).isEqualTo(1);

    // we are able to parse requests against multiple requests
    // however we throw an exception if that happens in practice
  }

  @Test
  public void testTraceSpanGeneratedTimestamp() throws IOException {
    String rawRequest = getRawQueryString("index_simple");

    List<IndexRequest> indexRequests = OpenSearchBulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);

    IngestDocument ingestDocument =
        OpenSearchBulkApiRequestParser.convertRequestToDocument(indexRequests.get(0));
    Trace.Span span = OpenSearchBulkApiRequestParser.fromIngestDocument(ingestDocument);

    // timestamp is in microseconds based on the trace.proto definition
    Instant ingestDocumentTime =
        Instant.ofEpochMilli(
            TimeUnit.MILLISECONDS.convert(span.getTimestamp(), TimeUnit.MICROSECONDS));
    Instant oneMinuteBefore = Instant.now().minus(1, ChronoUnit.MINUTES);
    assertThat(oneMinuteBefore.isBefore(ingestDocumentTime)).isTrue();
  }
}
