package com.slack.kaldb.schema;

import static com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser.convertRequestToDocument;
import static com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser.fromIngestDocument;
import static com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParserTest.getIndexRequestBytes;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser;
import com.slack.astra.proto.schema.Schema;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ingest.IngestDocument;

public class MultiFieldsTest {

  @Test
  public void testMultiFields() throws IOException {
    Map<String, Schema.SchemaField> fields = new HashMap<>();

    Schema.SchemaField amountMultiField =
        Schema.SchemaField.newBuilder()
            .setType(Schema.SchemaFieldType.FLOAT)
            .putFields(
                "keyword",
                Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.KEYWORD).build())
            .putFields(
                "integer",
                Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.INTEGER).build())
            .build();
    Schema.SchemaField messageMultiField =
        Schema.SchemaField.newBuilder()
            .setType(Schema.SchemaFieldType.TEXT)
            .putFields(
                "keyword",
                Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.KEYWORD).build())
            .build();

    fields.put("amount", amountMultiField);
    fields.put("message", messageMultiField);

    Schema.IngestSchema schema = Schema.IngestSchema.newBuilder().putAllFields(fields).build();

    byte[] rawRequest = getIndexRequestBytes("index_all_schema_fields");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);
    IngestDocument ingestDocument = convertRequestToDocument(indexRequests.get(0));

    Trace.Span span = fromIngestDocument(ingestDocument, schema);
    Map<String, Trace.KeyValue> tags =
        span.getTagsList().stream()
            .map(kv -> Map.entry(kv.getKey(), kv))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    assertThat(tags.get("amount").getFieldType()).isEqualTo(Schema.SchemaFieldType.FLOAT);
    assertThat(tags.get("amount").getVFloat32()).isEqualTo(1.1f);

    assertThat(tags.get("amount.keyword").getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(tags.get("amount.keyword").getVStr()).isEqualTo("1.1");

    // cannot parse 1.1 as integer
    assertThat(tags.get("failed_amount.integer").getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(tags.get("failed_amount.integer").getVStr()).isEqualTo("1.1");

    assertThat(tags.get("message").getFieldType()).isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(tags.get("message").getVStr()).isEqualTo("foo bar");

    assertThat(tags.get("message.keyword").getFieldType())
        .isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(tags.get("message.keyword").getVStr()).isEqualTo("foo bar");
  }
}
