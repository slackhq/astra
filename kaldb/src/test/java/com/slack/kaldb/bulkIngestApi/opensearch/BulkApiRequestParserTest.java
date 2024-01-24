package com.slack.kaldb.bulkIngestApi.opensearch;

import static com.slack.kaldb.bulkIngestApi.opensearch.BulkApiRequestParser.convertRequestToDocument;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.Resources;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ingest.IngestDocument;

public class BulkApiRequestParserTest {

  private byte[] getRawQueryBytes(String filename) throws IOException {
    return Resources.toString(
            Resources.getResource(String.format("opensearchRequest/bulk/%s.ndjson", filename)),
            Charset.defaultCharset())
        .getBytes(StandardCharsets.UTF_8);
  }

  @Test
  public void testSimpleIndexRequest() throws Exception {
    byte[] rawRequest = getRawQueryBytes("index_simple");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);
    assertThat(indexRequests.get(0).index()).isEqualTo("test");
    assertThat(indexRequests.get(0).id()).isEqualTo("1");
    assertThat(indexRequests.get(0).sourceAsMap().size()).isEqualTo(2);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
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
  public void testIndexNoFields() throws Exception {
    byte[] rawRequest = getRawQueryBytes("index_no_fields");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("test").size()).isEqualTo(1);

    assertThat(indexDocs.get("test").get(0).getId().toStringUtf8()).isEqualTo("1");
    assertThat(indexDocs.get("test").get(0).getTagsList().size()).isEqualTo(1);
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
  public void testIndexNoFieldsNoId() throws Exception {
    byte[] rawRequest = getRawQueryBytes("index_no_fields_no_id");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("test").size()).isEqualTo(1);

    assertThat(indexDocs.get("test").get(0).getId().toStringUtf8()).isNotNull();
    assertThat(indexDocs.get("test").get(0).getTagsList().size()).isEqualTo(1);
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
  public void testIndexEmptyRequest() throws Exception {
    byte[] rawRequest = getRawQueryBytes("index_empty_request");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(0);
  }

  @Test
  public void testOtherBulkRequests() throws Exception {
    byte[] rawRequest = getRawQueryBytes("non_index");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(0);
  }

  @Test
  public void testIndexRequestWithSpecialChars() throws Exception {
    byte[] rawRequest = getRawQueryBytes("index_request_with_special_chars");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);
    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("index_name").size()).isEqualTo(1);

    assertThat(indexDocs.get("index_name").get(0).getId().toStringUtf8()).isNotNull();
    assertThat(indexDocs.get("index_name").get(0).getTagsList().size()).isEqualTo(4);
    assertThat(
            indexDocs.get("index_name").get(0).getTagsList().stream()
                .filter(
                    keyValue ->
                        keyValue.getKey().equals("service_name")
                            && keyValue.getVStr().equals("index_name"))
                .count())
        .isEqualTo(1);
  }

  @Test
  public void testBulkRequests() throws Exception {
    byte[] rawRequest = getRawQueryBytes("bulk_requests");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(2);
    assertThat(indexDocs.get("test1").size()).isEqualTo(1);
    assertThat(indexDocs.get("test3").size()).isEqualTo(1);

    Trace.Span indexDoc1 = indexDocs.get("test1").get(0);
    Trace.Span indexDoc3 = indexDocs.get("test3").get(0);

    assertThat(indexDoc1.getId().toStringUtf8()).isEqualTo("1");
    assertThat(indexDoc3.getId().toStringUtf8()).isEqualTo("3");

    assertThat(indexDoc1.getTagsList().size()).isEqualTo(2);
    assertThat(
            indexDoc1.getTagsList().stream()
                .filter(
                    keyValue ->
                        keyValue.getKey().equals("service_name")
                            && keyValue.getVStr().equals("test1"))
                .count())
        .isEqualTo(1);

    assertThat(indexDoc3.getTagsList().size()).isEqualTo(2);
    assertThat(
            indexDoc3.getTagsList().stream()
                .filter(
                    keyValue ->
                        keyValue.getKey().equals("service_name")
                            && keyValue.getVStr().equals("test3"))
                .count())
        .isEqualTo(1);
  }

  @Test
  public void testUpdatesAgainstTwoIndexes() throws Exception {
    byte[] rawRequest = getRawQueryBytes("two_indexes");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(2);
    assertThat(indexDocs.get("test1").size()).isEqualTo(1);
    assertThat(indexDocs.get("test2").size()).isEqualTo(1);

    // we are able to parse requests against multiple requests
    // however we throw an exception if that happens in practice
  }

  @Test
  public void testTraceSpanGeneratedTimestamp() throws IOException {
    byte[] rawRequest = getRawQueryBytes("index_simple");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);

    IngestDocument ingestDocument = convertRequestToDocument(indexRequests.get(0));
    Trace.Span span = BulkApiRequestParser.fromIngestDocument(ingestDocument);

    // timestamp is in microseconds based on the trace.proto definition
    Instant ingestDocumentTime =
        Instant.ofEpochMilli(
            TimeUnit.MILLISECONDS.convert(span.getTimestamp(), TimeUnit.MICROSECONDS));
    Instant oneMinuteBefore = Instant.now().minus(1, ChronoUnit.MINUTES);
    assertThat(oneMinuteBefore.isBefore(ingestDocumentTime)).isTrue();
  }
}
