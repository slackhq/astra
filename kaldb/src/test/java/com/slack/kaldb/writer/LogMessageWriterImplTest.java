package com.slack.kaldb.writer;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.makeChunkManagerUtil;
import static com.slack.kaldb.testlib.MessageUtil.TEST_MESSAGE_TYPE;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.SpanUtil.makeSpan;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
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
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class LogMessageWriterImplTest {

  private static final String S3_TEST_BUCKET = "test-kaldb-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private SimpleMeterRegistry metricsRegistry;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        makeChunkManagerUtil(
            S3_MOCK_EXTENSION,
            S3_TEST_BUCKET,
            metricsRegistry,
            10 * 1024 * 1024 * 1024L,
            100,
            KaldbConfigUtil.makeIndexerConfig());
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @AfterEach
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
    final Instant timestamp = Instant.now();
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";

    SimpleMeterRegistry localMetricsRegistry = new SimpleMeterRegistry();
    ChunkManagerUtil<LogMessage> localChunkManagerUtil =
        makeChunkManagerUtil(
            S3_MOCK_EXTENSION,
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
                        TimeUnit.MICROSECONDS.convert(
                            timestamp.toEpochMilli() + i * 1000L, TimeUnit.MILLISECONDS),
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
    final Instant timestamp = Instant.now();
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";
    final String msgType = "test_message_type";
    final Trace.Span span =
        makeSpan(
            traceId,
            id,
            parentId,
            TimeUnit.MICROSECONDS.convert(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS),
            durationMicros,
            name,
            serviceName,
            msgType);
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
