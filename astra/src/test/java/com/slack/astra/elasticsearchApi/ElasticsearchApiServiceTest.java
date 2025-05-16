package com.slack.astra.elasticsearchApi;

import static com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser.convertRequestToDocument;
import static com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParserTest.getIndexRequestBytes;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.writer.LogMessageWriterImplTest.consumerRecordWithValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser;
import com.slack.astra.chunkManager.IndexingChunkManager;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.AstraLocalQueryService;
import com.slack.astra.metadata.schema.SchemaUtil;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import com.slack.astra.testlib.AstraConfigUtil;
import com.slack.astra.testlib.ChunkManagerUtil;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.writer.LogMessageWriterImpl;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ingest.IngestDocument;

@SuppressWarnings("UnstableApiUsage")
public class ElasticsearchApiServiceTest {
  private static final String S3_TEST_BUCKET = "test-astra-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private static final String TEST_KAFKA_PARTITION_ID = "10";

  private ElasticsearchApiService elasticsearchApiService;

  private SimpleMeterRegistry metricsRegistry;
  private ChunkManagerUtil<LogMessage> chunkManagerUtil;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        ChunkManagerUtil.makeChunkManagerUtil(
            S3_MOCK_EXTENSION,
            S3_TEST_BUCKET,
            metricsRegistry,
            10 * 1024 * 1024 * 1024L,
            1000000L,
            AstraConfigUtil.makeIndexerConfig());
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
    AstraLocalQueryService<LogMessage> searcher =
        new AstraLocalQueryService<>(chunkManagerUtil.chunkManager, Duration.ofSeconds(3));
    elasticsearchApiService = new ElasticsearchApiService(searcher);
  }

  @AfterEach
  public void tearDown() throws TimeoutException, IOException {
    chunkManagerUtil.close();
    metricsRegistry.close();
  }

  @Test
  public void testSchemaIsRetainedAndDynamicFieldsAreDroppedOverLimit() throws Exception {
    // Load schema from test_schema.yaml
    final File schemaFile =
        new File(getClass().getClassLoader().getResource("schema/test_schema.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    // Build a request with all schema fields (reusing test fixture) and extra dynamic fields
    byte[] rawRequest = getIndexRequestBytes("index_all_schema_fields");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);

    // Insert schema-based spans first
    for (IndexRequest indexRequest : indexRequests) {
      IngestDocument ingestDocument = convertRequestToDocument(indexRequest);
      Trace.Span span = BulkApiRequestParser.fromIngestDocument(ingestDocument, schema);
      ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());
      LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
      assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    }

    // Now add one large span with 3000 dynamic fields
    Trace.Span.Builder spanBuilder = SpanUtil.makeSpan(100).toBuilder();
    for (int i = 0; i < 3000; i++) {
      spanBuilder.addTags(
          Trace.KeyValue.newBuilder()
              .setKey("dynamic.extra_field." + i)
              .setVStr("value" + i)
              .setIndexSignal(Trace.IndexSignal.DYNAMIC_INDEX)
              .build());
    }
    Trace.Span spanWithExtras = spanBuilder.build();
    ConsumerRecord<String, byte[]> extraSpanRecord =
        consumerRecordWithValue(spanWithExtras.toByteArray());
    LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
    assertThat(messageWriter.insertRecord(extraSpanRecord)).isTrue();

    // Validate counts
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    // Fetch and parse mapping
    HttpResponse response =
        elasticsearchApiService.mapping(
            Optional.of("test"), Optional.of(0L), Optional.of(Long.MAX_VALUE));

    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);
    assertThat(jsonNode).isNotNull();

    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> map =
        objectMapper.convertValue(
            jsonNode.get("test").get("mappings").get("properties"), Map.class);

    Set<String> schemaKeys =
        Set.of(
            "host",
            "message",
            "ip",
            "my_date",
            "success",
            "cost",
            "amount",
            "amount_half_float",
            "message.keyword",
            "value",
            "count",
            "count_scaled_long",
            "count_short",
            "bucket");

    // Verify all original schema fields are retained
    assertThat(map.keySet()).containsAll(schemaKeys);

    // Additional Keys that are required via indexing logic.
    Set<String> requiredKeys =
        Set.of(
            "@timestamp",
            "_all",
            "_id",
            "_index",
            "_source",
            "_timesinceepoch",
            "doubleproperty",
            "duration",
            "floatproperty",
            "intproperty",
            "longproperty",
            "name",
            "parent_id",
            "service_name",
            "stringproperty",
            "trace_id",
            "binaryproperty");

    // Verify all required keys are retained
    assertThat(map.keySet()).containsAll(requiredKeys);

    int dynamicFieldsCount = map.keySet().size() - schemaKeys.size() - requiredKeys.size();
    assertThat(dynamicFieldsCount)
        .withFailMessage(
            "Expected dynamic field count to not exceed limit but got %s", dynamicFieldsCount)
        .isEqualTo(1500);
  }

  @Test
  public void testSchemaIsRetainedAndDynamicFieldsAreNotDroppedWhenUNKNOWN() throws Exception {
    // Load schema from test_schema.yaml
    final File schemaFile =
        new File(getClass().getClassLoader().getResource("schema/test_schema.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    // Build a request with all schema fields (reusing test fixture) and extra dynamic fields
    byte[] rawRequest = getIndexRequestBytes("index_all_schema_fields");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);

    // Insert schema-based spans first
    for (IndexRequest indexRequest : indexRequests) {
      IngestDocument ingestDocument = convertRequestToDocument(indexRequest);
      Trace.Span span = BulkApiRequestParser.fromIngestDocument(ingestDocument, schema);
      ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());
      LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
      assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    }

    // Now add one large span with 3000 dynamic fields
    Trace.Span.Builder spanBuilder = SpanUtil.makeSpan(100).toBuilder();
    for (int i = 0; i < 3000; i++) {
      spanBuilder.addTags(
          Trace.KeyValue.newBuilder()
              .setKey("dynamic.extra_field." + i)
              .setVStr("value" + i)
              .build());
    }
    Trace.Span spanWithExtras = spanBuilder.build();
    ConsumerRecord<String, byte[]> extraSpanRecord =
        consumerRecordWithValue(spanWithExtras.toByteArray());
    LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
    assertThat(messageWriter.insertRecord(extraSpanRecord)).isTrue();

    // Validate counts
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    // Fetch and parse mapping
    HttpResponse response =
        elasticsearchApiService.mapping(
            Optional.of("test"), Optional.of(0L), Optional.of(Long.MAX_VALUE));

    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);
    assertThat(jsonNode).isNotNull();

    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> map =
        objectMapper.convertValue(
            jsonNode.get("test").get("mappings").get("properties"), Map.class);

    Set<String> schemaKeys =
        Set.of(
            "host",
            "message",
            "ip",
            "my_date",
            "success",
            "cost",
            "amount",
            "amount_half_float",
            "value",
            "count",
            "count_scaled_long",
            "count_short",
            "bucket");

    // Verify all original schema fields are retained
    assertThat(map.keySet()).containsAll(schemaKeys);

    // Additional Keys that are required via indexing logic.
    Set<String> requiredKeys =
        Set.of(
            "@timestamp",
            "_all",
            "_id",
            "_index",
            "_source",
            "_timesinceepoch",
            "doubleproperty",
            "duration",
            "floatproperty",
            "intproperty",
            "longproperty",
            "message.keyword",
            "name",
            "parent_id",
            "service_name",
            "stringproperty",
            "trace_id",
            "username",
            "binaryproperty");

    // Verify all required keys are retained
    assertThat(map.keySet()).containsAll(requiredKeys);

    // No dynamic fields are dropped with the "old" logic when preprocessor does not have
    // indexSignal set.
    int dynamicFieldsCount = map.keySet().size() - schemaKeys.size() - requiredKeys.size();
    assertThat(dynamicFieldsCount)
        .withFailMessage(
            "Expected dynamic field count to not exceed limit but got %s", dynamicFieldsCount)
        .isEqualTo(3000);
  }

  @Test
  public void testSchemaFields() throws Exception {
    final File schemaFile =
        new File(getClass().getClassLoader().getResource("schema/test_schema.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    byte[] rawRequest = getIndexRequestBytes("index_all_schema_fields");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);

    for (IndexRequest indexRequest : indexRequests) {
      IngestDocument ingestDocument = convertRequestToDocument(indexRequest);
      Trace.Span span = BulkApiRequestParser.fromIngestDocument(ingestDocument, schema);
      ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());
      LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
      assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    }

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    HttpResponse response =
        elasticsearchApiService.mapping(
            Optional.of("test"), Optional.of(0L), Optional.of(Long.MAX_VALUE));

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);
    assertThat(jsonNode).isNotNull();

    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> map =
        objectMapper.convertValue(
            jsonNode.get("test").get("mappings").get("properties"), Map.class);
    assertThat(map).isNotNull();
    assertThat(map.size()).isEqualTo(25);
  }

  // todo - test mapping
  @Test
  public void testResultsAreReturnedForValidQuery() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    String postBody =
        Resources.toString(
            Resources.getResource("elasticsearchApi/multisearch_query_500results.ndjson"),
            Charset.defaultCharset());
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(100);
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(0)
                .findValue("message")
                .asText()
                .endsWith("Message100"))
        .isTrue();
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(99)
                .findValue("message")
                .asText()
                .endsWith("Message1"))
        .isTrue();
  }

  @Test
  public void testSearchStringWithOneResult() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    String postBody =
        Resources.toString(
            Resources.getResource("elasticsearchApi/multisearch_query_1results.ndjson"),
            Charset.defaultCharset());
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(1);
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(0)
                .findValue("message")
                .asText()
                .endsWith("Message70"))
        .isTrue();
  }

  @Test
  public void testSearchStringWithNoResult() throws Exception {
    // add 100 results around now
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    // queries for 1 second duration in year 2056
    String postBody =
        Resources.toString(
            Resources.getResource("elasticsearchApi/multisearch_query_0results.ndjson"),
            Charset.defaultCharset());
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(0);
  }

  @Test
  public void testResultSizeIsRespected() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    String postBody =
        Resources.toString(
            Resources.getResource("elasticsearchApi/multisearch_query_10results.ndjson"),
            Charset.defaultCharset());
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(10);
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(0)
                .findValue("message")
                .asText()
                .endsWith("Message100"))
        .isTrue();
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(9)
                .findValue("message")
                .asText()
                .endsWith("Message91"))
        .isTrue();
  }

  @Test
  public void testLargeSetOfQueries() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));
    String postBody =
        Resources.toString(
            Resources.getResource("elasticsearchApi/multisearch_query_10results.ndjson"),
            Charset.defaultCharset());
    AstraLocalQueryService<LogMessage> slowSearcher =
        spy(new AstraLocalQueryService<>(chunkManagerUtil.chunkManager, Duration.ofSeconds(5)));

    // warmup to load OpenSearch plugins
    ElasticsearchApiService slowElasticsearchApiService = new ElasticsearchApiService(slowSearcher);
    slowElasticsearchApiService.multiSearch(postBody);

    slowElasticsearchApiService = new ElasticsearchApiService(slowSearcher);
    HttpResponse response = slowElasticsearchApiService.multiSearch(postBody.repeat(100));

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);

    // ensure we have all 100 results
    assertThat(jsonNode.get("responses").size()).isEqualTo(100);
  }

  @Test
  public void testEmptySearchGrafana7() throws Exception {
    String postBody =
        Resources.toString(
            Resources.getResource("elasticsearchApi/empty_search_grafana7.ndjson"),
            Charset.defaultCharset());
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(0);
  }

  @Test
  public void testEmptySearchGrafana8() throws Exception {
    String postBody =
        Resources.toString(
            Resources.getResource("elasticsearchApi/empty_search_grafana8.ndjson"),
            Charset.defaultCharset());
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(0);
  }

  @Test
  public void testIndexMapping() throws IOException {
    AstraQueryServiceBase searcher = mock(AstraQueryServiceBase.class);
    ElasticsearchApiService serviceUnderTest = new ElasticsearchApiService(searcher);

    Instant start = Instant.now();
    Instant end = start.minusSeconds(60);

    when(searcher.getSchema(
            eq(
                AstraSearch.SchemaRequest.newBuilder()
                    .setDataset("foo")
                    .setStartTimeEpochMs(start.toEpochMilli())
                    .setEndTimeEpochMs(end.toEpochMilli())
                    .build())))
        .thenReturn(AstraSearch.SchemaResult.newBuilder().build());

    HttpResponse response =
        serviceUnderTest.mapping(
            Optional.of("foo"), Optional.of(start.toEpochMilli()), Optional.of(end.toEpochMilli()));
    verify(searcher)
        .getSchema(
            eq(
                AstraSearch.SchemaRequest.newBuilder()
                    .setDataset("foo")
                    .setStartTimeEpochMs(start.toEpochMilli())
                    .setEndTimeEpochMs(end.toEpochMilli())
                    .build()));

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);

    assertThat(jsonNode.findValue("foo")).isNotNull();
    assertThat(
            jsonNode.findValue("foo").findValue(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName))
        .isNotNull();

    when(searcher.getSchema(any()))
        .thenAnswer(
            invocationOnMock -> {
              AstraSearch.SchemaRequest request =
                  ((AstraSearch.SchemaRequest) invocationOnMock.getArguments()[0]);
              assertThat(request.getDataset()).isEqualTo("bar");
              assertThat(request.getStartTimeEpochMs())
                  .isCloseTo(
                      Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli(),
                      Offset.offset(1000L));
              assertThat(request.getEndTimeEpochMs())
                  .isCloseTo(Instant.now().toEpochMilli(), Offset.offset(1000L));
              return AstraSearch.SchemaResult.newBuilder().build();
            });
    serviceUnderTest.mapping(Optional.of("bar"), Optional.empty(), Optional.empty());
  }

  private void addMessagesToChunkManager(List<Trace.Span> messages) throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();
  }
}
