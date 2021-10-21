package com.slack.kaldb.writer;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.testlib.MessageUtil.TEST_INDEX_NAME;
import static com.slack.kaldb.testlib.MessageUtil.getCurrentLogDate;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.SpanUtil.makeSpan;
import static com.slack.kaldb.testlib.SpanUtil.makeSpanBuilder;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LogMessageWriterImplTest {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private SimpleMeterRegistry metricsRegistry;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    KaldbConfigUtil.initEmptyIndexerConfig();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        new ChunkManagerUtil<>(S3_MOCK_RULE, metricsRegistry, 10 * 1024 * 1024 * 1024L, 100);
  }

  @After
  public void tearDown() throws IOException, TimeoutException {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
    metricsRegistry.close();
  }

  private SearchResult<LogMessage> searchChunkManager(String indexName, String queryString) {
    return chunkManagerUtil
        .chunkManager
        .query(new SearchQuery(indexName, queryString, 0, MAX_TIME, 10, 1000))
        .join();
  }

  @Test
  public void testJSONLogMessageInsertion() throws IOException {
    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.jsonLogMessageTransformer);

    String jsonLogMessge = MessageUtil.makeLogMessageJSON(1);
    ConsumerRecord<String, byte[]> jsonRecord = consumerRecordWithValue(jsonLogMessge.getBytes());

    assertThat(messageWriter.insertRecord(jsonRecord)).isTrue();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    // Search
    assertThat(searchChunkManager(TEST_INDEX_NAME, "").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(TEST_INDEX_NAME, "Message1").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(TEST_INDEX_NAME, "Message2").hits.size()).isEqualTo(0);
    assertThat(searchChunkManager(TEST_INDEX_NAME, "id:Message1").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(TEST_INDEX_NAME, "intproperty:1").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(TEST_INDEX_NAME, "intproperty:2").hits.size()).isEqualTo(0);
    assertThat(searchChunkManager(TEST_INDEX_NAME, "longproperty:1 AND intproperty:1").hits.size())
        .isEqualTo(1);
  }

  @Test
  public void testFaultyJSONLogMessageInsertion() throws IOException {
    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.jsonLogMessageTransformer);

    Map<String, Object> fieldMap = Maps.newHashMap();
    String id = "1";
    fieldMap.put("id", id);
    fieldMap.put("index", TEST_INDEX_NAME);
    Map<String, Object> sourceFieldMap = new HashMap<>();
    sourceFieldMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, getCurrentLogDate());
    String message = String.format("The identifier in this message is %s", id);
    sourceFieldMap.put(LogMessage.ReservedField.MESSAGE.fieldName, message);
    fieldMap.put("source", sourceFieldMap);
    String jsonLogMessage = new ObjectMapper().writeValueAsString(fieldMap);

    ConsumerRecord<String, byte[]> jsonRecord = consumerRecordWithValue(jsonLogMessage.getBytes());
    assertThat(messageWriter.insertRecord(jsonRecord)).isFalse();
  }

  @Test
  public void testMalformedJSONLogMessageInsertion() throws IOException {
    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.jsonLogMessageTransformer);

    ConsumerRecord<String, byte[]> jsonRecord =
        consumerRecordWithValue("malformedJsonMessage".getBytes());
    assertThat(messageWriter.insertRecord(jsonRecord)).isFalse();
  }

  @Test
  public void testApiLogMessageInsertion() throws Exception {
    // Make a test message
    String message =
        "{\"ip_address\":\"3.86.63.133\",\"http_method\":\"POST\",\"method\":\"callbacks.flannel.verifyToken\",\"enterprise\":\"E012Y1ZD5PU\",\"team\":\"T012YTS8XKM\",\"user\":\"U012YTS942X\",\"status\":\"ok\",\"http_params\":\"flannel_host=flannelbe-dev-iad-iaz2&include_permissions=falseth\",\"ua\":\"Slack-Flannel-Web\\/vef2bd:4046\",\"unique_id\":\"YB2RcPgcUv7PCuIbo8posQAAoDg\",\"request_queue_time\":2262,\"microtime_elapsed\":14168,\"mysql_query_count\":0,\"mysql_query_time\":0,\"mysql_conns_count\":0,\"mysql_conns_time\":0,\"mysql_rows_count\":0,\"mysql_rows_affected\":0,\"mc_queries_count\":11,\"mc_queries_time\":6782,\"frl_time\":0,\"init_time\":1283,\"api_dispatch_time\":0,\"api_output_time\":0,\"api_output_size\":0,\"api_strict\":false,\"ekm_decrypt_reqs_time\":0,\"ekm_decrypt_reqs_count\":0,\"ekm_encrypt_reqs_time\":0,\"ekm_encrypt_reqs_count\":0,\"grpc_req_count\":0,\"grpc_req_time\":0,\"agenda_req_count\":0,\"agenda_req_time\":0,\"trace\":\"#route_main() -> lib_controller.php:69#Controller::handlePost() -> Controller.hack:58#CallbackApiController::handleRequest() -> api.php:45#local_callbacks_api_main_inner() -> api.php:250#api_dispatch() -> lib_api.php:179#api_callbacks_flannel_verifyToken() -> api__callbacks_flannel.php:1714#api_output_fb_thrift() -> lib_api_output.php:390#_api_output_log_call()\",\"client_connection_state\":\"unset\",\"ms_requests_count\":0,\"ms_requests_time\":0,\"token_type\":\"cookie\",\"limited_access_requester_workspace\":\"\",\"limited_access_allowed_workspaces\":\"\",\"repo_auth\":true,\"cf_id\":\"6999afc7b6:haproxy-edge-dev-iad-igu2\",\"external_user\":\"W012XXXFC\",\"timestamp\":\"2021-02-05 10:41:52.340\",\"git_sha\":\"unknown\",\"hhvm_version\":\"4.39.0\",\"slath\":\"callbacks.flannel.verifyToken\",\"php_type\":\"api\",\"webapp_cluster_pbucket\":0,\"webapp_cluster_name\":\"callbacks\",\"webapp_cluster_nest\":\"normal\",\"dev_env\":\"dev-main\",\"pay_product_level\":\"enterprise\",\"type\":\"api_log\",\"level\":\"info\"}";
    String indexName = "hhvm-api_log";
    String host = "slack-www-hhvm-dev-dev-callbacks-iad-j8zj";
    long timestamp = 1612550512340953000L;
    Murron.MurronMessage testMurronMsg =
        Murron.MurronMessage.newBuilder()
            .setMessage(ByteString.copyFrom(message.getBytes(StandardCharsets.UTF_8)))
            .setType(indexName)
            .setHost(host)
            .setTimestamp(timestamp)
            .build();
    ConsumerRecord<String, byte[]> apiRecord = consumerRecordWithMurronMessage(testMurronMsg);

    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.apiLogTransformer);

    // Insert and search.
    assertThat(messageWriter.insertRecord(apiRecord)).isTrue();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    final String testIndex = "hhvm_api_log";
    assertThat(searchChunkManager(testIndex, "").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(testIndex, "http_method:POST").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(testIndex, "http_method:GET").hits.size()).isEqualTo(0);
    assertThat(searchChunkManager(testIndex, "method:callbacks*").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(testIndex, "http_method:POST AND method:callbacks*").hits.size())
        .isEqualTo(1);
    assertThat(searchChunkManager(testIndex, "http_method:GET AND method:callbacks*").hits.size())
        .isEqualTo(0);
    assertThat(searchChunkManager(testIndex, "http_method:GET OR method:callbacks*").hits.size())
        .isEqualTo(1);
  }

  private static ConsumerRecord<String, byte[]> consumerRecordWithMurronMessage(
      Murron.MurronMessage testMurronMsg) {
    return consumerRecordWithValue(testMurronMsg.toByteString().toByteArray());
  }

  private static ConsumerRecord<String, byte[]> consumerRecordWithValue(byte[] recordValue) {
    return new ConsumerRecord<>(
        "testTopic", 1, 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "testKey", recordValue);
  }

  @Test
  public void insertNullRecord() throws IOException {
    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.apiLogTransformer);

    assertThat(messageWriter.insertRecord(null)).isFalse();
  }

  @Test
  public void testMalformedMurronApiRecord() throws IOException {
    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.apiLogTransformer);

    ConsumerRecord<String, byte[]> apiRecord =
        consumerRecordWithMurronMessage(
            Murron.MurronMessage.newBuilder()
                .setMessage(ByteString.copyFromUtf8("malformedMurronMessage"))
                .setType("testIndex")
                .setHost("testHost")
                .setTimestamp(1612550512340953000L)
                .build());

    assertThat(messageWriter.insertRecord(apiRecord)).isFalse();
  }

  @Test
  public void testMalformedMurronSpanRecord() throws IOException {
    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.spanTransformer);

    ConsumerRecord<String, byte[]> spanRecord =
        consumerRecordWithMurronMessage(
            Murron.MurronMessage.newBuilder()
                .setMessage(ByteString.copyFromUtf8("malformedMurronMessage"))
                .setType("testIndex")
                .setHost("testHost")
                .setTimestamp(1612550512340953000L)
                .build());

    assertThat(messageWriter.insertRecord(spanRecord)).isFalse();
  }

  @Test
  public void testSpanLogMessageInsertion() throws IOException {
    // Data Prep: Span -> ListOfSpans -> MurronMessage -> ConsumerReord
    final String traceId = "t1";
    final String id = "i1";
    final String parentId = "p2";
    final long timestampMicros = 1612550512340953L;
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";
    final String msgType = "test_message_type";
    final Trace.Span span =
        makeSpan(
            traceId, id, parentId, timestampMicros, durationMicros, name, serviceName, msgType);

    ConsumerRecord<String, byte[]> spanRecord =
        consumerRecordWithMurronMessage(
            Murron.MurronMessage.newBuilder()
                .setMessage(Trace.ListOfSpans.newBuilder().addSpans(span).build().toByteString())
                .setType("testIndex")
                .setHost("testHost")
                .setTimestamp(1612550512340953000L)
                .build());

    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.spanTransformer);

    assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    assertThat(searchChunkManager(serviceName, "").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:POST").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "type:test_message_type").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "service_name:test_service").hits.size())
        .isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:GET").hits.size()).isEqualTo(0);
    assertThat(searchChunkManager(serviceName, "method:callbacks*").hits.size()).isEqualTo(1);
    assertThat(
            searchChunkManager(serviceName, "http_method:POST AND method:callbacks*").hits.size())
        .isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:GET AND method:callbacks*").hits.size())
        .isEqualTo(0);
    assertThat(searchChunkManager(serviceName, "http_method:GET OR method:callbacks*").hits.size())
        .isEqualTo(1);
  }

  @Test
  public void testMultipleSpanLogMessageInsertion() throws IOException {
    // Data Prep: Span -> ListOfSpans -> MurronMessage -> ConsumerReord
    final String traceId = "t1";
    final int id = 1;
    final long timestampMicros = 1612550512340953L;
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "test_span";
    final Trace.Span span1 =
        makeSpan(
            traceId,
            String.valueOf(id),
            "0",
            timestampMicros,
            durationMicros,
            name,
            serviceName,
            SpanFormatter.DEFAULT_LOG_MESSAGE_TYPE);
    final Trace.Span span2 =
        makeSpan(
            traceId,
            String.valueOf(id + 1),
            String.valueOf(id),
            timestampMicros,
            durationMicros,
            name + "2",
            serviceName,
            SpanFormatter.DEFAULT_LOG_MESSAGE_TYPE);

    List<Trace.Span> spans = List.of(span1, span2);
    ByteString serializedMessage =
        Trace.ListOfSpans.newBuilder().addAllSpans(spans).build().toByteString();
    ConsumerRecord<String, byte[]> spanRecord =
        consumerRecordWithMurronMessage(
            Murron.MurronMessage.newBuilder()
                .setMessage(serializedMessage)
                .setType("testIndex")
                .setHost("testHost")
                .setTimestamp(1612550512340953000L)
                .build());

    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.spanTransformer);

    assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    assertThat(searchChunkManager(serviceName, "").hits.size()).isEqualTo(2);
    assertThat(searchChunkManager(serviceName, "http_method:POST").hits.size()).isEqualTo(2);
    assertThat(searchChunkManager(serviceName, "trace_id:t1").hits.size()).isEqualTo(2);
    assertThat(searchChunkManager(serviceName, "id:1").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "id:2").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "parent_id:1").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "parent_id:0").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "name:test_span").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:POST AND name:test_span").hits.size())
        .isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:POST AND name:test_span2").hits.size())
        .isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:POST AND name:test_span*").hits.size())
        .isEqualTo(2);
    assertThat(searchChunkManager(serviceName, "http_method:GET").hits.size()).isEqualTo(0);
    assertThat(searchChunkManager(serviceName, "method:callbacks*").hits.size()).isEqualTo(2);
    assertThat(
            searchChunkManager(serviceName, "http_method:POST AND method:callbacks*").hits.size())
        .isEqualTo(2);
    assertThat(searchChunkManager(serviceName, "http_method:GET AND method:callbacks*").hits.size())
        .isEqualTo(0);
    assertThat(searchChunkManager(serviceName, "http_method:GET OR method:callbacks*").hits.size())
        .isEqualTo(2);
  }

  @Test
  public void testIngestSpanListWithErrorSpan() throws IOException {
    // Data Prep: Span -> ListOfSpans -> MurronMessage -> ConsumerReord
    final String traceId = "t1";
    final long timestampMicros = 1612550512340953L;
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "test_span";
    final String msgType = "msg_type";
    final Trace.Span span1 =
        makeSpan(traceId, "1", "0", timestampMicros, durationMicros, name, serviceName, msgType);

    final Trace.Span.Builder spanBuilder =
        makeSpanBuilder(
            traceId, "2", "1", timestampMicros, durationMicros, name, serviceName, msgType);
    // Add a tag that violates property type.
    spanBuilder.addTags(
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setVTypeValue(Trace.ValueType.INT64.getNumber())
            .setVInt64(100)
            .build());
    final Trace.Span span2 = spanBuilder.build();
    List<Trace.Span> spans = List.of(span1, span2);
    ByteString serializedMessage =
        Trace.ListOfSpans.newBuilder().addAllSpans(spans).build().toByteString();
    ConsumerRecord<String, byte[]> spanRecord =
        consumerRecordWithMurronMessage(
            Murron.MurronMessage.newBuilder()
                .setMessage(serializedMessage)
                .setType("testIndex")
                .setHost("testHost")
                .setTimestamp(1612550512340953000L)
                .build());

    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.spanTransformer);

    assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(1);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    assertThat(searchChunkManager(serviceName, "").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:POST").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "trace_id:t1").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "id:1").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "id:2").hits.size()).isZero();
    assertThat(searchChunkManager(serviceName, "parent_id:0").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:GET OR method:callbacks*").hits.size())
        .isEqualTo(1);
  }

  @Test
  public void testInsertSpanWithPropertyTypeViolation() throws Exception {
    final String traceId = "t1";
    final String id = "i1";
    final String parentId = "p2";
    final String msgType = "test_message_type";
    final long timestampMicros = 1612550512340953L;
    final long durationMicros = 500000L;
    final String serviceName = "testService";
    final String name = "testSpanName";
    final Trace.Span.Builder spanBuilder =
        makeSpanBuilder(
            traceId, id, parentId, timestampMicros, durationMicros, name, serviceName, msgType);
    // Add a tag that violates property type.
    spanBuilder.addTags(
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setVTypeValue(Trace.ValueType.INT64.getNumber())
            .setVInt64(100)
            .build());
    final Trace.Span span = spanBuilder.build();
    ConsumerRecord<String, byte[]> spanRecord =
        consumerRecordWithMurronMessage(
            Murron.MurronMessage.newBuilder()
                .setMessage(Trace.ListOfSpans.newBuilder().addSpans(span).build().toByteString())
                .setType("testIndex")
                .setHost("testHost")
                .setTimestamp(1612550512340953000L)
                .build());

    List<LogMessage> logMessages = LogMessageWriterImpl.spanTransformer.toLogMessage(spanRecord);
    assertThat(logMessages.size()).isEqualTo(1);

    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.spanTransformer);

    assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(1);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();
  }

  @Test
  public void testAvgMessageSizeCalculationOnSpanIngestion() throws Exception {
    final String traceId = "t1";
    final long timestampMicros = 1612550512340953L;
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";
    final String msgType = "test_message_type";

    SimpleMeterRegistry localMetricsRegistry = new SimpleMeterRegistry();
    ChunkManagerUtil<LogMessage> localChunkManagerUtil =
        new ChunkManagerUtil<>(S3_MOCK_RULE, localMetricsRegistry, 1000L, 100);

    List<Trace.Span> spans =
        IntStream.range(0, 15)
            .mapToObj(
                i ->
                    makeSpan(
                        traceId,
                        String.valueOf(i),
                        "0",
                        timestampMicros,
                        durationMicros,
                        name,
                        serviceName,
                        msgType))
            .collect(Collectors.toList());

    Murron.MurronMessage testMurronMsg =
        Murron.MurronMessage.newBuilder()
            .setMessage(Trace.ListOfSpans.newBuilder().addAllSpans(spans).build().toByteString())
            .setType("testIndex")
            .setHost("testHost")
            .setTimestamp(1612550512340953000L)
            .build();

    ConsumerRecord<String, byte[]> spanRecord =
        new ConsumerRecord<>(
            "testTopic",
            1,
            10,
            0L,
            TimestampType.CREATE_TIME,
            0L,
            10,
            1500,
            "testKey",
            testMurronMsg.toByteString().toByteArray());

    IndexingChunkManager<LogMessage> chunkManager = localChunkManagerUtil.chunkManager;
    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(chunkManager, LogMessageWriterImpl.spanTransformer);

    assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, localMetricsRegistry)).isEqualTo(15);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, localMetricsRegistry)).isEqualTo(0);
    localChunkManagerUtil.chunkManager.getActiveChunk().commit();
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);

    assertThat(
            chunkManager
                .query(new SearchQuery(serviceName, "", 0, MAX_TIME, 100, 1000))
                .join()
                .hits
                .size())
        .isEqualTo(15);
  }

  @Test
  public void testUseIncorrectDataTransformer() throws IOException {
    // Data Prep: Span -> ListOfSpans -> MurronMessage -> ConsumerReord
    final String traceId = "t1";
    final String id = "i2";
    final String parentId = "p2";
    final long timestampMicros = 1612550512340953L;
    final long durationMicros = 500000L;
    final String serviceName = "testService";
    final String name = "testSpanName";

    final Trace.Span span =
        makeSpan(
            traceId,
            id,
            parentId,
            timestampMicros,
            durationMicros,
            name,
            serviceName,
            SpanFormatter.DEFAULT_LOG_MESSAGE_TYPE);

    Murron.MurronMessage testMurronMsg =
        Murron.MurronMessage.newBuilder()
            .setMessage(
                Trace.ListOfSpans.newBuilder().addAllSpans(List.of(span)).build().toByteString())
            .setType("test")
            .setHost("testHost")
            .setTimestamp(timestampMicros)
            .build();
    ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithMurronMessage(testMurronMsg);

    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.apiLogTransformer);

    assertThat(messageWriter.insertRecord(spanRecord)).isFalse();
  }
}
