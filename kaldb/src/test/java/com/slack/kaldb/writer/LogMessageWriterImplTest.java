package com.slack.kaldb.writer;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.makeChunkManagerUtil;
import static com.slack.kaldb.testlib.MessageUtil.TEST_MESSAGE_TYPE;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.SpanUtil.makeSpan;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.protobuf.ByteString;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
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

  private static final String S3_TEST_BUCKET = "test-kaldb-logs";

  @ClassRule
  public static final S3MockRule S3_MOCK_RULE =
      S3MockRule.builder().withInitialBuckets(S3_TEST_BUCKET).silent().build();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private SimpleMeterRegistry metricsRegistry;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        makeChunkManagerUtil(
            S3_MOCK_RULE,
            S3_TEST_BUCKET,
            metricsRegistry,
            10 * 1024 * 1024 * 1024L,
            100,
            KaldbConfigUtil.makeIndexerConfig());
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @After
  public void tearDown() throws IOException, TimeoutException {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
    metricsRegistry.close();
  }

  private SearchResult<LogMessage> searchChunkManager(String indexName, String queryString) {
    return chunkManagerUtil.chunkManager.query(
        new SearchQuery(
            indexName,
            queryString,
            0,
            MAX_TIME,
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
            Collections.emptyList()),
        Duration.ofMillis(3000));
  }

  @Test
  public void testApiLogMessageInsertion() throws Exception {
    // Make a test message
    String message =
        "{\"ip_address\":\"3.86.63.133\",\"http_method\":\"POST\",\"method\":\"callbacks.flannel.verifyToken\",\"enterprise\":\"E012Y1ZD5PU\",\"team\":\"T012YTS8XKM\",\"user\":\"U012YTS942X\",\"status\":\"ok\",\"http_params\":\"flannel_host=flannelbe-dev-iad-iaz2&include_permissions=falseth\",\"ua\":\"Slack-Flannel-Web\\/vef2bd:4046\",\"unique_id\":\"YB2RcPgcUv7PCuIbo8posQAAoDg\",\"request_queue_time\":2262,\"microtime_elapsed\":14168,\"mysql_query_count\":0,\"mysql_query_time\":0,\"mysql_conns_count\":0,\"mysql_conns_time\":0,\"mysql_rows_count\":0,\"mysql_rows_affected\":0,\"mc_queries_count\":11,\"mc_queries_time\":6782,\"frl_time\":0,\"init_time\":1283,\"api_dispatch_time\":0,\"api_output_time\":0,\"api_output_size\":0,\"api_strict\":false,\"ekm_decrypt_reqs_time\":0,\"ekm_decrypt_reqs_count\":0,\"ekm_encrypt_reqs_time\":0,\"ekm_encrypt_reqs_count\":0,\"grpc_req_count\":0,\"grpc_req_time\":0,\"agenda_req_count\":0,\"agenda_req_time\":0,\"trace\":\"#route_main() -> lib_controller.php:69#Controller::handlePost() -> Controller.hack:58#CallbackApiController::handleRequest() -> api.php:45#local_callbacks_api_main_inner() -> api.php:250#api_dispatch() -> lib_api.php:179#api_callbacks_flannel_verifyToken() -> api__callbacks_flannel.php:1714#api_output_fb_thrift() -> lib_api_output.php:390#_api_output_log_call()\",\"client_connection_state\":\"unset\",\"ms_requests_count\":0,\"ms_requests_time\":0,\"token_type\":\"cookie\",\"limited_access_requester_workspace\":\"\",\"limited_access_allowed_workspaces\":\"\",\"repo_auth\":true,\"cf_id\":\"6999afc7b6:haproxy-edge-dev-iad-igu2\",\"external_user\":\"W012XXXFC\",\"timestamp\":\"2021-02-05 10:41:52.340\",\"git_sha\":\"unknown\",\"hhvm_version\":\"4.39.0\",\"slath\":\"callbacks.flannel.verifyToken\",\"php_type\":\"api\",\"webapp_cluster_pbucket\":0,\"webapp_cluster_name\":\"callbacks\",\"webapp_cluster_nest\":\"normal\",\"dev_env\":\"dev-main\",\"pay_product_level\":\"enterprise\",\"level\":\"info\"}";
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

  // TODO: Add a unit test where message fails to index. Can't do it now since the field conflict
  // policy is hard-coded.

  @Test
  public void testAvgMessageSizeCalculationOnSpanIngestion() throws Exception {
    final String traceId = "t1";
    final long timestampMicros = 1612550512340953L;
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";

    SimpleMeterRegistry localMetricsRegistry = new SimpleMeterRegistry();
    ChunkManagerUtil<LogMessage> localChunkManagerUtil =
        makeChunkManagerUtil(
            S3_MOCK_RULE,
            S3_TEST_BUCKET,
            localMetricsRegistry,
            1000L,
            100,
            KaldbConfigUtil.makeIndexerConfig());
    localChunkManagerUtil.chunkManager.startAsync();
    localChunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    List<Trace.Span> spans =
        IntStream.range(0, 15)
            .mapToObj(
                i ->
                    makeSpan(
                        traceId,
                        String.valueOf(i),
                        "0",
                        timestampMicros + ((long) i * 1000),
                        durationMicros,
                        name,
                        serviceName,
                        TEST_MESSAGE_TYPE))
            .collect(Collectors.toList());

    IndexingChunkManager<LogMessage> chunkManager = localChunkManagerUtil.chunkManager;
    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(chunkManager, LogMessageWriterImpl.traceSpanTransformer);

    for (Trace.Span span : spans) {
      ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());
      assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    }

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, localMetricsRegistry)).isEqualTo(15);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, localMetricsRegistry)).isEqualTo(0);
    localChunkManagerUtil.chunkManager.getActiveChunk().commit();

    // even though we may have over 1k bytes, we only update the bytes ingested every 10s
    // as such this will only have 1 chunk at the time of query
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);

    assertThat(
            chunkManager
                .query(
                    new SearchQuery(
                        serviceName,
                        "",
                        0,
                        MAX_TIME,
                        100,
                        new DateHistogramAggBuilder(
                            "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
                        Collections.emptyList()),
                    Duration.ofMillis(3000))
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

  @Test
  public void testIngestTraceSpan() throws IOException {
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
    ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());

    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.traceSpanTransformer);

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
  public void testNullTraceSpan() throws IOException {
    LogMessageWriterImpl messageWriter =
        new LogMessageWriterImpl(
            chunkManagerUtil.chunkManager, LogMessageWriterImpl.traceSpanTransformer);

    assertThat(messageWriter.insertRecord(null)).isFalse();
  }
}
