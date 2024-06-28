package com.slack.astra.bulkIngestApi.opensearch;

import static com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser.convertRequestToDocument;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.Resources;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.proto.schema.Schema;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.index.VersionType;
import org.opensearch.ingest.IngestDocument;

public class BulkApiRequestParserTest {

  public static byte[] getIndexRequestBytes(String filename) throws IOException {
    return Resources.toString(
            Resources.getResource(String.format("opensearchRequest/bulk/%s.ndjson", filename)),
            Charset.defaultCharset())
        .getBytes(StandardCharsets.UTF_8);
  }

  @Test
  public void testSimpleIndexRequestWithMicros() throws Exception {
    byte[] rawRequest = getIndexRequestBytes("index_simple_with_micro_ts");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);
    assertThat(indexRequests.get(0).index()).isEqualTo("test");
    assertThat(indexRequests.get(0).id()).isEqualTo("1");
    assertThat(indexRequests.get(0).sourceAsMap().size()).isEqualTo(3);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(
            indexRequests, Schema.IngestSchema.newBuilder().build());
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("test").size()).isEqualTo(1);

    assertThat(indexDocs.get("test").get(0).getId().toStringUtf8()).isEqualTo("1");
    assertThat(indexDocs.get("test").get(0).getTagsList().size()).isEqualTo(4);
    assertThat(
            indexDocs.get("test").get(0).getTagsList().stream()
                .filter(
                    keyValue ->
                        keyValue.getKey().equals("service_name")
                            && keyValue.getVStr().equals("test"))
                .count())
        .isEqualTo(1);
    assertThat(indexDocs.get("test").get(0).getTimestamp()).isEqualTo(4739680479544123L);
  }

  @Test
  public void testSimpleIndexRequest() throws Exception {
    byte[] rawRequest = getIndexRequestBytes("index_simple_with_ts");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);
    assertThat(indexRequests.get(0).index()).isEqualTo("test");
    assertThat(indexRequests.get(0).id()).isEqualTo("1");
    assertThat(indexRequests.get(0).sourceAsMap().size()).isEqualTo(3);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(
            indexRequests, Schema.IngestSchema.newBuilder().build());
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("test").size()).isEqualTo(1);

    assertThat(indexDocs.get("test").get(0).getId().toStringUtf8()).isEqualTo("1");
    assertThat(indexDocs.get("test").get(0).getTagsList().size()).isEqualTo(4);
    assertThat(
            indexDocs.get("test").get(0).getTagsList().stream()
                .filter(
                    keyValue ->
                        keyValue.getKey().equals("service_name")
                            && keyValue.getVStr().equals("test"))
                .count())
        .isEqualTo(1);
    assertThat(indexDocs.get("test").get(0).getTimestamp()).isEqualTo(4739680479544000L);
  }

  @Test
  public void testIndexNoFields() throws Exception {
    byte[] rawRequest = getIndexRequestBytes("index_no_fields");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(
            indexRequests, Schema.IngestSchema.newBuilder().build());
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
    byte[] rawRequest = getIndexRequestBytes("index_no_fields_no_id");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(
            indexRequests, Schema.IngestSchema.newBuilder().build());
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
    byte[] rawRequest = getIndexRequestBytes("index_empty_request");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(
            indexRequests, Schema.IngestSchema.newBuilder().build());
    assertThat(indexDocs.keySet().size()).isEqualTo(0);
  }

  @Test
  public void testOtherBulkRequests() throws Exception {
    byte[] rawRequest = getIndexRequestBytes("non_index");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(0);
  }

  @Test
  public void testIndexRequestWithSpecialChars() throws Exception {
    byte[] rawRequest = getIndexRequestBytes("index_request_with_special_chars");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);
    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(
            indexRequests, Schema.IngestSchema.newBuilder().build());
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
    byte[] rawRequest = getIndexRequestBytes("bulk_requests");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(
            indexRequests, Schema.IngestSchema.newBuilder().build());
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
    byte[] rawRequest = getIndexRequestBytes("two_indexes");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);

    Map<String, List<Trace.Span>> indexDocs =
        BulkApiRequestParser.convertIndexRequestToTraceFormat(
            indexRequests, Schema.IngestSchema.newBuilder().build());
    assertThat(indexDocs.keySet().size()).isEqualTo(2);
    assertThat(indexDocs.get("test1").size()).isEqualTo(1);
    assertThat(indexDocs.get("test2").size()).isEqualTo(1);

    // we are able to parse requests against multiple requests
    // however we throw an exception if that happens in practice
  }

  @Test
  public void testSchemaFieldForTags() throws IOException {
    byte[] rawRequest = getIndexRequestBytes("index_simple");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);

    Schema.SchemaField type1 =
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.KEYWORD).build();
    Schema.SchemaField type2 =
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.TEXT).build();
    Schema.IngestSchema schema =
        Schema.IngestSchema.newBuilder()
            .putFields("field1", type1)
            .putFields("field2", type2)
            .build();

    IngestDocument ingestDocument = convertRequestToDocument(indexRequests.get(0));
    Trace.Span span = BulkApiRequestParser.fromIngestDocument(ingestDocument, schema);

    List<Trace.KeyValue> field1Def =
        span.getTagsList().stream().filter(keyValue -> keyValue.getKey().equals("field1")).toList();
    assertThat(field1Def.size()).isEqualTo(1);
    assertThat(field1Def.getFirst().getVStr()).isEqualTo("value1");
    assertThat(field1Def.getFirst().getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);

    field1Def =
        span.getTagsList().stream().filter(keyValue -> keyValue.getKey().equals("field2")).toList();
    assertThat(field1Def.size()).isEqualTo(1);
    assertThat(field1Def.getFirst().getVStr()).isEqualTo("value2");
    assertThat(field1Def.getFirst().getFieldType()).isEqualTo(Schema.SchemaFieldType.TEXT);

    field1Def =
        span.getTagsList().stream()
            .filter(keyValue -> keyValue.getKey().equals("service_name"))
            .toList();
    assertThat(field1Def.size()).isEqualTo(1);
    assertThat(field1Def.getFirst().getVStr()).isEqualTo("test");
    assertThat(field1Def.getFirst().getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
  }

  @Test
  public void testTraceSpanGeneratedTimestamp() throws IOException {
    byte[] rawRequest = getIndexRequestBytes("index_simple");

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(1);

    IngestDocument ingestDocument = convertRequestToDocument(indexRequests.get(0));
    Trace.Span span =
        BulkApiRequestParser.fromIngestDocument(
            ingestDocument, Schema.IngestSchema.newBuilder().build());

    // timestamp is in microseconds based on the trace.proto definition
    Instant ingestDocumentTime =
        Instant.ofEpochMilli(
            TimeUnit.MILLISECONDS.convert(span.getTimestamp(), TimeUnit.MICROSECONDS));
    Instant oneMinuteBefore = Instant.now().minus(1, ChronoUnit.MINUTES);
    assertThat(oneMinuteBefore.isBefore(ingestDocumentTime)).isTrue();

    Instant oneMinuteAfter = Instant.now().plus(1, ChronoUnit.MINUTES);
    assertThat(ingestDocumentTime.isBefore(oneMinuteAfter)).isTrue();
  }

  @Test
  public void testNullDocumentIdFromIngestDocument() {
    IngestDocument nullId =
        new IngestDocument("index", null, "routing", 1L, VersionType.INTERNAL, Map.of());
    Trace.Span nullIdTrace =
        BulkApiRequestParser.fromIngestDocument(nullId, Schema.IngestSchema.newBuilder().build());

    assertThat(nullIdTrace.getId().toStringUtf8()).isNotNull();
    assertThat(nullIdTrace.getId().toStringUtf8()).isNotEmpty();
    assertThat(nullIdTrace.getId().toStringUtf8()).isNotEqualToIgnoringCase("null");
  }

  @Test
  public void testEmptyDocumentIdFromIngestDocument() {
    IngestDocument emptyId =
        new IngestDocument("index", "", "routing", 1L, VersionType.INTERNAL, Map.of());
    Trace.Span emptyIdTrace =
        BulkApiRequestParser.fromIngestDocument(emptyId, Schema.IngestSchema.newBuilder().build());

    assertThat(emptyIdTrace.getId().toStringUtf8()).isNotNull();
    assertThat(emptyIdTrace.getId().toStringUtf8()).isNotEmpty();
    assertThat(emptyIdTrace.getId().toStringUtf8()).isNotEqualToIgnoringCase("null");
  }

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public void testEmptyIndexFromIngestDocument() {
    IngestDocument emptyIndex =
        new IngestDocument(
            "", UUID.randomUUID().toString(), "routing", 1L, VersionType.INTERNAL, Map.of());
    Trace.Span emptyIndexTrace =
        BulkApiRequestParser.fromIngestDocument(
            emptyIndex, Schema.IngestSchema.newBuilder().build());
    assertThat(
            emptyIndexTrace.getTagsList().stream()
                .filter(tag -> tag.getKey().equals("service_name"))
                .findFirst()
                .get()
                .getVStr())
        .isEqualTo("default");
  }

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public void testNullIndexFromINgestDocument() {
    IngestDocument nullIndex =
        new IngestDocument(
            null, UUID.randomUUID().toString(), "routing", 1L, VersionType.INTERNAL, Map.of());
    Trace.Span nullIndexTrace =
        BulkApiRequestParser.fromIngestDocument(
            nullIndex, Schema.IngestSchema.newBuilder().build());
    assertThat(
            nullIndexTrace.getTagsList().stream()
                .filter(tag -> tag.getKey().equals("service_name"))
                .findFirst()
                .get()
                .getVStr())
        .isEqualTo("default");
  }

  @Test
  public void testMalformedReservedFieldsFromIngestDocument() {
    IngestDocument malformedReservedFields =
        new IngestDocument(
            "index",
            UUID.randomUUID().toString(),
            "routing",
            1L,
            VersionType.INTERNAL,
            Map.of(
                LogMessage.ReservedField.PARENT_ID.fieldName,
                1L,
                LogMessage.ReservedField.TRACE_ID.fieldName,
                true,
                LogMessage.ReservedField.NAME.fieldName,
                2.2,
                LogMessage.ReservedField.DURATION.fieldName,
                "yes"));
    Trace.Span malformedReservedFieldsTrace =
        BulkApiRequestParser.fromIngestDocument(
            malformedReservedFields, Schema.IngestSchema.newBuilder().build());

    assertThat(malformedReservedFieldsTrace.getParentId().toStringUtf8()).isEqualTo("1");
    assertThat(malformedReservedFieldsTrace.getTraceId().toStringUtf8()).isEqualTo("true");
    assertThat(malformedReservedFieldsTrace.getName()).isEqualTo("2.2");
    assertThat(malformedReservedFieldsTrace.getDuration()).isEqualTo(0);
  }

  @Test
  public void testTimestampParsingFromIngestDocument() {
    IngestDocument ingestDocument =
        new IngestDocument("index", "1", "routing", 1L, VersionType.INTERNAL, Map.of());
    long timeInMicros =
        BulkApiRequestParser.getTimestampFromIngestDocument(ingestDocument.getSourceAndMetadata());

    // this tests that the parser inserted a timestamp close to the current time
    long oneMinuteBefore =
        ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now().minus(1, ChronoUnit.MINUTES));
    long oneMinuteAfter =
        ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now().plus(1, ChronoUnit.MINUTES));
    assertThat(oneMinuteBefore <= timeInMicros).isTrue();
    assertThat(timeInMicros <= oneMinuteAfter).isTrue();

    // We respect the user provided @timestamp field
    String ts = "2024-01-01T00:00:00.000Z";
    Instant providedTimeStamp = Instant.parse(ts);
    ingestDocument =
        new IngestDocument(
            "index", "1", "routing", 1L, VersionType.INTERNAL, Map.of("@timestamp", ts));
    timeInMicros =
        BulkApiRequestParser.getTimestampFromIngestDocument(ingestDocument.getSourceAndMetadata());
    assertThat(timeInMicros).isEqualTo(ChronoUnit.MICROS.between(Instant.EPOCH, providedTimeStamp));

    // we put a long in the @timestamp field, which today we don't parse
    // so it won't be 2024-01-01 but be the current timestamp
    ingestDocument =
        new IngestDocument(
            "index",
            "1",
            "routing",
            1L,
            VersionType.INTERNAL,
            Map.of("@timestamp", providedTimeStamp.toEpochMilli()));
    timeInMicros =
        BulkApiRequestParser.getTimestampFromIngestDocument(ingestDocument.getSourceAndMetadata());
    assertThat(oneMinuteBefore <= timeInMicros).isTrue();
    assertThat(timeInMicros <= oneMinuteAfter).isTrue();

    // we put a string in the timestamp field, which today we don't parse
    // so it won't be 2024-01-01 but be the current timestamp
    ingestDocument =
        new IngestDocument(
            "index", "1", "routing", 1L, VersionType.INTERNAL, Map.of("_timestamp", ts));
    timeInMicros =
        BulkApiRequestParser.getTimestampFromIngestDocument(ingestDocument.getSourceAndMetadata());
    assertThat(oneMinuteBefore <= timeInMicros).isTrue();
    assertThat(timeInMicros <= oneMinuteAfter).isTrue();

    // we put a string in the _timestamp field, which today we don't parse
    // so it won't be 2024-01-01 but be the current timestamp
    ingestDocument =
        new IngestDocument(
            "index", "1", "routing", 1L, VersionType.INTERNAL, Map.of("timestamp", ts));
    timeInMicros =
        BulkApiRequestParser.getTimestampFromIngestDocument(ingestDocument.getSourceAndMetadata());
    assertThat(oneMinuteBefore <= timeInMicros).isTrue();
    assertThat(timeInMicros <= oneMinuteAfter).isTrue();
  }
}
