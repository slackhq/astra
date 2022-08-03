package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.makeChunkManagerUtil;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.zipkinApi.ZipkinService.BINARY_TAG_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.protobuf.ByteString;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.util.JsonUtil;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.*;

public class KaldbLocalQueryServiceTest {
  private static final String TEST_KAFKA_PARITION_ID = "10";
  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private KaldbLocalQueryService<LogMessage> kaldbLocalQueryService;
  private SimpleMeterRegistry metricsRegistry;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        makeChunkManagerUtil(
            S3_MOCK_RULE,
            metricsRegistry,
            10 * 1024 * 1024 * 1024L,
            100,
            KaldbConfigUtil.makeIndexerConfig(1000, 1000, "log_message", 100));
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
    kaldbLocalQueryService = new KaldbLocalQueryService<>(chunkManagerUtil.chunkManager);
  }

  @After
  public void tearDown() throws IOException, TimeoutException {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
  }

  @Test
  public void testKalDbSearch() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(100);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (100 * 1000);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    KaldbSearch.SearchResult response =
        kaldbLocalQueryService.doSearch(
            searchRequestBuilder
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("Message100")
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setBucketCount(2)
                .build());

    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getTotalCount()).isEqualTo(1);
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);

    // Test hit contents
    assertThat(response.getHits(0)).contains("Message100");
    List<ByteString> hits = response.getHitsList().asByteStringList();
    assertThat(hits.size()).isEqualTo(1);
    LogWireMessage hit = JsonUtil.read(hits.get(0).toStringUtf8(), LogWireMessage.class);
    LogMessage m = LogMessage.fromWireMessage(hit);
    assertThat(m.getType()).isEqualTo(MessageUtil.TEST_MESSAGE_TYPE);
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_INDEX_NAME);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(100);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(100);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(100.0);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(100.0);
    assertThat((String) m.source.get("message")).contains("Message100");

    // Test histogram buckets
    assertThat(response.getBucketsList().size()).isEqualTo(2);
    KaldbSearch.HistogramBucket bucket1 = response.getBuckets(0);
    assertThat(bucket1.getCount()).isEqualTo(0);
    assertThat(bucket1.getLow()).isEqualTo(chunk1StartTimeMs);
    assertThat(bucket1.getHigh()).isEqualTo((chunk1StartTimeMs + chunk1EndTimeMs) / 2.0);
    KaldbSearch.HistogramBucket bucket2 = response.getBuckets(1);
    assertThat(bucket2.getCount()).isEqualTo(1);
    assertThat(bucket2.getHigh()).isEqualTo(chunk1EndTimeMs);

    // TODO: Query multiple chunks.
  }

  @Test
  public void testKalDbSearchNoData() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    KaldbSearch.SearchResult response =
        kaldbLocalQueryService.doSearch(
            searchRequestBuilder
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("blah")
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setBucketCount(2)
                .build());

    assertThat(response.getHitsCount()).isZero();
    assertThat(response.getTotalCount()).isZero();
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getTotalCount()).isZero();
    assertThat(response.getHitsList().asByteStringList().size()).isZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);

    // Test histogram buckets
    assertThat(response.getBucketsList().size()).isEqualTo(2);
    KaldbSearch.HistogramBucket bucket1 = response.getBuckets(0);
    assertThat(bucket1.getCount()).isEqualTo(0);
    assertThat(bucket1.getLow()).isEqualTo(chunk1StartTimeMs);
    assertThat(bucket1.getHigh()).isEqualTo((chunk1StartTimeMs + chunk1EndTimeMs) / 2.0);
    KaldbSearch.HistogramBucket bucket2 = response.getBuckets(1);
    assertThat(bucket2.getCount()).isEqualTo(0);
    assertThat(bucket2.getHigh()).isEqualTo(chunk1EndTimeMs);
  }

  @Test
  public void testKalDbSearchNoHits() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    // TODO: Query multiple chunks.
    KaldbSearch.SearchResult response =
        kaldbLocalQueryService.doSearch(
            searchRequestBuilder
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("Message1")
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(0)
                .setBucketCount(2)
                .build());

    // Count is 0, but totalCount is 1, since there is 1 hit, but none are to be retrieved.
    assertThat(response.getHitsCount()).isEqualTo(0);
    assertThat(response.getTotalCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);
    assertThat(response.getHitsList().asByteStringList().size()).isZero();

    // Test histogram buckets
    assertThat(response.getBucketsList().size()).isEqualTo(2);
    KaldbSearch.HistogramBucket bucket1 = response.getBuckets(0);
    assertThat(bucket1.getCount()).isEqualTo(1);
    assertThat(bucket1.getLow()).isEqualTo(chunk1StartTimeMs);
    assertThat(bucket1.getHigh()).isEqualTo((chunk1StartTimeMs + chunk1EndTimeMs) / 2.0);
    KaldbSearch.HistogramBucket bucket2 = response.getBuckets(1);
    assertThat(bucket2.getCount()).isEqualTo(0);
    assertThat(bucket2.getHigh()).isEqualTo(chunk1EndTimeMs);
  }

  @Test
  public void testKalDbSearchNoHistogram() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    KaldbSearch.SearchResult response =
        kaldbLocalQueryService.doSearch(
            searchRequestBuilder
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("Message1")
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setBucketCount(0)
                .build());

    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTotalCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);

    // Test hit contents
    assertThat(response.getHitsList().asByteStringList().size()).isEqualTo(1);
    assertThat(response.getHits(0)).contains("Message1");
    List<ByteString> hits = response.getHitsList().asByteStringList();
    assertThat(hits.size()).isEqualTo(1);
    LogWireMessage hit = JsonUtil.read(hits.get(0).toStringUtf8(), LogWireMessage.class);
    LogMessage m = LogMessage.fromWireMessage(hit);
    assertThat(m.getType()).isEqualTo(MessageUtil.TEST_MESSAGE_TYPE);
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_INDEX_NAME);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(1);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(1);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(1.0);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(1.0);
    assertThat((String) m.source.get("message")).contains("Message1");

    // Test histogram buckets
    assertThat(response.getBucketsList().size()).isEqualTo(0);
  }

  @Test(expected = RuntimeException.class)
  public void testKalDbBadArgSearch() throws Throwable {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    kaldbLocalQueryService.doSearch(
        searchRequestBuilder
            .setIndexName(MessageUtil.TEST_INDEX_NAME)
            .setQueryString("Message1")
            .setStartTimeEpochMs(chunk1StartTimeMs)
            .setEndTimeEpochMs(chunk1EndTimeMs)
            .setHowMany(0)
            .setBucketCount(0)
            .build());
  }

  @Test
  public void testKalDbGrpcSearch() throws IOException {
    // Load test data into chunk manager.
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    // Setup a InProcess Grpc Server so we can query it.
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();

    // Create a server, add service, start, and register for automatic graceful shutdown.
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new KaldbLocalQueryService<>(chunkManager))
            .build()
            .start());

    // Create a client channel and register for automatic graceful shutdown.
    KaldbServiceGrpc.KaldbServiceBlockingStub blockingKaldbClient =
        KaldbServiceGrpc.newBlockingStub(
            // Create a client channel and register for automatic graceful shutdown.
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    // Build a search request
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);
    KaldbSearch.SearchResult response =
        blockingKaldbClient.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("Message1")
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setBucketCount(2)
                .build());

    // Validate search response
    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getTotalCount()).isEqualTo(1);
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);

    // Test hit contents
    assertThat(response.getHits(0)).contains("Message1");
    List<ByteString> hits = response.getHitsList().asByteStringList();
    assertThat(hits.size()).isEqualTo(1);
    LogWireMessage hit = JsonUtil.read(hits.get(0).toStringUtf8(), LogWireMessage.class);
    LogMessage m = LogMessage.fromWireMessage(hit);
    assertThat(m.getType()).isEqualTo(MessageUtil.TEST_MESSAGE_TYPE);
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_INDEX_NAME);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(1);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(1);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(1.0);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(1.0);
    assertThat((String) m.source.get("message")).contains("Message1");

    // Test histogram buckets
    assertThat(response.getBucketsList().size()).isEqualTo(2);
    KaldbSearch.HistogramBucket bucket1 = response.getBuckets(0);
    assertThat(bucket1.getCount()).isEqualTo(1);
    assertThat(bucket1.getLow()).isEqualTo(chunk1StartTimeMs);
    assertThat(bucket1.getHigh()).isEqualTo((chunk1StartTimeMs + chunk1EndTimeMs) / 2.0);
    KaldbSearch.HistogramBucket bucket2 = response.getBuckets(1);
    assertThat(bucket2.getCount()).isEqualTo(0);
    assertThat(bucket2.getHigh()).isEqualTo(chunk1EndTimeMs);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testKalDbGrpcSearchThrowsException() throws IOException {
    // Load test data into chunk manager.
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    // Setup a InProcess Grpc Server so we can query it.
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();

    // Create a server, add service, start, and register for automatic graceful shutdown.
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new KaldbLocalQueryService<>(chunkManager))
            .build()
            .start());

    // Create a client channel and register for automatic graceful shutdown.
    KaldbServiceGrpc.KaldbServiceBlockingStub blockingStub =
        KaldbServiceGrpc.newBlockingStub(
            // Create a client channel and register for automatic graceful shutdown.
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    // Build a bad search request.
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);
    KaldbSearch.SearchResult result =
        blockingStub.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("Message1")
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(0)
                .setBucketCount(0)
                .build());
  }

  @Test
  public void zipkinGetTracesTimestampTest() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> insertMessages =
        MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (LogMessage m : insertMessages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    Long endTs = 1601547099000L;
    Long lookback = 1601547092000L;
    int limit = 10;
    String queryString = "id:Message*";
    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();
    KaldbSearch.SearchResult searchResult =
        kaldbLocalQueryService.doSearch(
            searchRequestBuilder
                .setIndexName("testindex") // [Q] as of now we don't need to worry about index?
                .setQueryString(queryString) // query everything
                // startTime: endTs - lookback (conversion)
                .setStartTimeEpochMs(
                    lookback) // [Q] double check that these correspond to lookback and endTs not
                // min and max Duration
                .setEndTimeEpochMs(endTs)
                // [Q] difference between howmany and bucketcount?
                .setHowMany(limit)
                .setBucketCount(0)
                .build());
    List<LogMessage> messages = searchResultToLogMessage(searchResult);
    List<String> messageStrings = new ArrayList<>();

    for (LogMessage message : messages) {
      Map<String, Object> source = message.getSource();
      for (String k : source.keySet()) {}

      final String messageTraceId = (String) source.get("trace_id");
      final String messageId = message.id;
      final String messageParentId = (String) source.get("parent_id");
      // [Q]timestamp not completely correct
      final String messageTimestamp = (String) source.get("@timestamp");
      Instant instant = Instant.parse(messageTimestamp);
      final long messageTimestampMicros =
          TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
              + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
      final long messageDurationMicros = ((Number) source.get("duration_ms")).longValue();
      final String messageServiceName = (String) source.get("service_name");
      final String messageName = (String) source.get("name");
      // [Q]what to put for msgtype
      final String messageMsgType = "test message type";
      final com.slack.service.murron.trace.Trace.Span messageSpan =
          makeSpan(
              messageTraceId,
              messageId,
              messageParentId,
              messageTimestampMicros,
              messageDurationMicros,
              messageName,
              messageServiceName,
              messageMsgType);
      // NEED TO CONVERT A LIST OF TRACES
      messageStrings.add(JsonUtil.writeJsonArray(JsonUtil.writeAsString(messageSpan)));
    }

    System.out.println(messageStrings);
  }

  @Test
  public void zipkinGetTraceByIdTest() throws IOException {

    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2022, 7, 8, 13, 50, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> insertMessages =
        MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (LogMessage m : insertMessages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);

      offset++;
    }
    long defaultLookback = 86400000L;

    String queryString = "traceId:" + "sdfadhjfadshj";
    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();
    KaldbSearch.SearchResult searchResult =
        kaldbLocalQueryService.doSearch(
            searchRequestBuilder
                .setIndexName("testindex") // [Q] as of now we don't need to worry about index?
                .setQueryString(queryString) // query everything
                // startTime: endTs - lookback (conversion)
                .setStartTimeEpochMs(
                    defaultLookback) // [Q] double check that these correspond to lookback and endTs
                // not
                // [Q] what to set for this
                .setEndTimeEpochMs(System.currentTimeMillis())
                // [Q] difference between howmany and bucketcount?
                .setHowMany(10)
                .setBucketCount(0)
                .build());
    List<LogMessage> messages = searchResultToLogMessage(searchResult);
    List<String> messageStrings = new ArrayList<>();

    for (LogMessage message : messages) {
      Map<String, Object> source = message.getSource();
      final String messageTraceId = (String) source.get("trace_id");
      final String messageId = message.id;
      final String messageParentId = (String) source.get("parent_id");
      // [Q]timestamp not completely correct
      final String messageTimestamp = (String) source.get("@timestamp");
      Instant instant = Instant.parse(messageTimestamp);
      final long messageTimestampMicros =
          TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
              + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
      final long messageDurationMicros = ((Number) source.get("duration_ms")).longValue();
      final String messageServiceName = (String) source.get("service_name");
      final String messageName = (String) source.get("name");
      // [Q]what to put for msgtype
      final String messageMsgType = "test message type";
      final com.slack.service.murron.trace.Trace.Span messageSpan =
          makeSpan(
              messageTraceId,
              messageId,
              messageParentId,
              messageTimestampMicros,
              messageDurationMicros,
              messageName,
              messageServiceName,
              messageMsgType);
      // NEED TO CONVERT A LIST OF TRACES
      messageStrings.add(JsonUtil.writeJsonArray(JsonUtil.writeAsString(messageSpan)));
    }

    System.out.println(String.valueOf(messageStrings));
  }

  public static List<LogMessage> searchResultToLogMessage(KaldbSearch.SearchResult searchResult)
      throws IOException {
    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<LogMessage> messages = new ArrayList<>();
    for (ByteString byteString : hitsByteList) {
      LogWireMessage hit = JsonUtil.read(byteString.toStringUtf8(), LogWireMessage.class);
      LogMessage message = LogMessage.fromWireMessage(hit);
      messages.add(message);
    }
    return messages;
  }

  public static com.slack.service.murron.trace.Trace.Span makeSpan(
      String traceId,
      String id,
      String parentId,
      long timestampMicros,
      long durationMicros,
      String name,
      String serviceName,
      String msgType) {
    com.slack.service.murron.trace.Trace.Span.Builder spanBuilder =
        makeSpanBuilder(
            traceId, id, parentId, timestampMicros, durationMicros, name, serviceName, msgType);
    return spanBuilder.build();
  }

  public static com.slack.service.murron.trace.Trace.Span.Builder makeSpanBuilder(
      String traceId,
      String id,
      String parentId,
      long timestampMicros,
      long durationMicros,
      String name,
      String serviceName,
      String msgType) {
    com.slack.service.murron.trace.Trace.Span.Builder spanBuilder =
        com.slack.service.murron.trace.Trace.Span.newBuilder();
    spanBuilder.setTraceId(ByteString.copyFrom(traceId.getBytes()));
    spanBuilder.setId(ByteString.copyFrom(id.getBytes()));
    spanBuilder.setParentId(ByteString.copyFrom(parentId.getBytes()));
    spanBuilder.setStartTimestampMicros(timestampMicros);
    spanBuilder.setDurationMicros(durationMicros);
    spanBuilder.setName(name);

    List<com.slack.service.murron.trace.Trace.KeyValue> tags = new ArrayList<>();
    // Set service tag
    tags.add(
        com.slack.service.murron.trace.Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.SERVICE_NAME.fieldName)
            .setVTypeValue(com.slack.service.murron.trace.Trace.ValueType.STRING.getNumber())
            .setVStr(serviceName)
            .build());

    tags.add(
        com.slack.service.murron.trace.Trace.KeyValue.newBuilder()
            .setKey("http_method")
            .setVTypeValue(com.slack.service.murron.trace.Trace.ValueType.STRING.getNumber())
            .setVStr("POST")
            .build());

    tags.add(
        com.slack.service.murron.trace.Trace.KeyValue.newBuilder()
            .setKey("method")
            .setVTypeValue(com.slack.service.murron.trace.Trace.ValueType.STRING.getNumber())
            .setVStr("callbacks.flannel")
            .build());

    tags.add(
        com.slack.service.murron.trace.Trace.KeyValue.newBuilder()
            .setKey("boolean")
            .setVTypeValue(com.slack.service.murron.trace.Trace.ValueType.BOOL.getNumber())
            .setVBool(true)
            .build());

    tags.add(
        com.slack.service.murron.trace.Trace.KeyValue.newBuilder()
            .setKey("int")
            .setVTypeValue(com.slack.service.murron.trace.Trace.ValueType.INT64.getNumber())
            .setVInt64(1000)
            .setVFloat64(1001.2)
            .build());

    tags.add(
        com.slack.service.murron.trace.Trace.KeyValue.newBuilder()
            .setKey("float")
            .setVTypeValue(com.slack.service.murron.trace.Trace.ValueType.FLOAT64.getNumber())
            .setVFloat64(1001.2)
            .setVInt64(1000)
            .build());

    tags.add(
        com.slack.service.murron.trace.Trace.KeyValue.newBuilder()
            .setKey("binary")
            .setVTypeValue(com.slack.service.murron.trace.Trace.ValueType.BINARY.getNumber())
            .setVBinary(ByteString.copyFromUtf8(BINARY_TAG_VALUE))
            .setVStr("ignored")
            .build());

    tags.add(
        com.slack.service.murron.trace.Trace.KeyValue.newBuilder()
            .setKey(LogMessage.SystemField.TYPE.fieldName)
            .setVTypeValue(com.slack.service.murron.trace.Trace.ValueType.STRING.getNumber())
            .setVStr(msgType)
            .build());

    spanBuilder.addAllTags(tags);
    return spanBuilder;
  }
}
