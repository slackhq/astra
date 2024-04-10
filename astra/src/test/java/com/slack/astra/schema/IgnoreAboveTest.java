package com.slack.astra.schema;

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

public class IgnoreAboveTest {

  @Test
  public void testIgnoreAbove() throws IOException {
    Map<String, Schema.SchemaField> fields = new HashMap<>();

    Schema.SchemaField messageMultiField =
        Schema.SchemaField.newBuilder()
            .setType(Schema.SchemaFieldType.TEXT)
            .putFields(
                "keyword",
                Schema.SchemaField.newBuilder()
                    .setType(Schema.SchemaFieldType.KEYWORD)
                    .setIgnoreAbove(2)
                    .build())
            .build();

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

    assertThat(tags.get("message").getFieldType()).isEqualTo(Schema.SchemaFieldType.TEXT);
    assertThat(tags.get("message").getVStr()).isEqualTo("foo bar");

    assertThat(tags.get("message.keyword")).isNull();
  }
}
