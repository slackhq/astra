package com.slack.astra.logstore.search;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.ChunkManagerUtil.makeChunkManagerUtil;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.google.protobuf.ByteString;
import com.slack.astra.chunkManager.IndexingChunkManager;
import com.slack.astra.chunkManager.RollOverChunkTask;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.logstore.opensearch.OpenSearchInternalAggregation;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.proto.service.AstraServiceGrpc;
import com.slack.astra.testlib.AstraConfigUtil;
import com.slack.astra.testlib.ChunkManagerUtil;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.util.GrpcCleanupExtension;
import com.slack.astra.util.JsonUtil;
import com.slack.service.murron.trace.Trace;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;

public class AstraLocalQueryServiceTest {
  private static final String TEST_KAFKA_PARITION_ID = "10";
  private static final String S3_TEST_BUCKET = "test-astra-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  @RegisterExtension public final GrpcCleanupExtension grpcCleanup = new GrpcCleanupExtension();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private AstraLocalQueryService<LogMessage> astraLocalQueryService;
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
            AstraConfigUtil.makeIndexerConfig(1000, 1000, 100));
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
    astraLocalQueryService =
        new AstraLocalQueryService<>(chunkManagerUtil.chunkManager, Duration.ofSeconds(3));
  }

  @AfterEach
  public void tearDown() throws IOException, TimeoutException {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
  }

  private static String buildHistogramRequestJSON(long startMs, long endMs, int numBuckets) {
    String histogramRequest =
        """
        {"%s":{"date_histogram":{"interval":"%ds","field":"%s","min_doc_count":"%d","extended_bounds":{"min":1676498801027,"max":1676500240688},"format":"epoch_millis","offset":"5s"},"aggs":{}}}
    """
            .formatted(
                "1",
                (endMs - startMs) / numBuckets,
                LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                1);
    return histogramRequest;
  }

  private static String buildQueryFromQueryString(
      String queryString, Long startTime, Long endTime) {
    return "{\"bool\":{\"filter\":[{\"range\":{\"_timesinceepoch\":{\"gte\":%d,\"lte\":%d,\"format\":\"epoch_millis\"}}},{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"%s\"}}]}}"
        .formatted(startTime, endTime, queryString);
  }

  @Test
  public void testAstraSearch() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset, false);
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

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    AstraSearch.SearchResult response =
        astraLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(
                    buildQueryFromQueryString("Message100", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregationJson(
                    buildHistogramRequestJSON(chunk1StartTimeMs, chunk1EndTimeMs, 2))
                .build());

    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
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
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_DATASET_NAME);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(100);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(100);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(100.0);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(100.0);
    assertThat((String) m.getSource().get("message")).contains("Message100");

    // Test histogram buckets
    InternalDateHistogram dateHistogram =
        (InternalDateHistogram)
            OpenSearchInternalAggregation.fromByteArray(
                response.getInternalAggregations().toByteArray());
    assertThat(dateHistogram.getBuckets().size()).isEqualTo(1);
    assertThat(dateHistogram.getBuckets().get(0).getDocCount()).isEqualTo(1);

    // TODO: Query multiple chunks.
  }

  @Test
  public void testAstraSearchNoData() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset, false);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    AstraSearch.SearchResult response =
        astraLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(buildQueryFromQueryString("blah", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregationJson(
                    buildHistogramRequestJSON(chunk1StartTimeMs, chunk1EndTimeMs, 2))
                .build());

    assertThat(response.getHitsCount()).isZero();
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getHitsList().asByteStringList().size()).isZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);

    // Test histogram buckets
    InternalDateHistogram dateHistogram =
        (InternalDateHistogram)
            OpenSearchInternalAggregation.fromByteArray(
                response.getInternalAggregations().toByteArray());
    assertThat(dateHistogram.getBuckets().size()).isEqualTo(0);
  }

  @Test
  public void testAstraSearchNoHits() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset, false);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    // TODO: Query multiple chunks.
    AstraSearch.SearchResult response =
        astraLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(buildQueryFromQueryString("Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(0)
                .setAggregationJson(
                    buildHistogramRequestJSON(chunk1StartTimeMs, chunk1EndTimeMs, 2))
                .build());

    assertThat(response.getHitsCount()).isEqualTo(0);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);
    assertThat(response.getHitsList().asByteStringList().size()).isZero();

    // Test histogram buckets
    InternalDateHistogram dateHistogram =
        (InternalDateHistogram)
            OpenSearchInternalAggregation.fromByteArray(
                response.getInternalAggregations().toByteArray());
    assertThat(dateHistogram.getBuckets().size()).isEqualTo(1);
    assertThat(dateHistogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testAstraSearchNoHistogram() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset, false);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    AstraSearch.SearchResult response =
        astraLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(buildQueryFromQueryString("Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregationJson("")
                .build());

    assertThat(response.getHitsCount()).isEqualTo(1);
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
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_DATASET_NAME);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(1);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(1);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(1.0);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(1.0);
    assertThat((String) m.getSource().get("message")).contains("Message1");

    // Test histogram buckets
    assertThat(response.getInternalAggregations().size()).isEqualTo(0);
  }

  @Test
  public void testAstraBadArgSearch() throws Throwable {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset, false);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(
            () ->
                astraLocalQueryService.doSearch(
                    searchRequestBuilder
                        .setDataset(MessageUtil.TEST_DATASET_NAME)
                        .setQuery(
                            buildQueryFromQueryString(
                                "Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                        .setStartTimeEpochMs(chunk1StartTimeMs)
                        .setEndTimeEpochMs(chunk1EndTimeMs)
                        .setHowMany(0)
                        .setAggregationJson("")
                        .build()));
  }

  @Test
  public void testAstraGrpcSearch() throws IOException {
    // Load test data into chunk manager.
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset, false);
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
            .addService(new AstraLocalQueryService<>(chunkManager, Duration.ofSeconds(3)))
            .build()
            .start());

    // Create a client channel and register for automatic graceful shutdown.
    AstraServiceGrpc.AstraServiceBlockingStub blockingAstraClient =
        AstraServiceGrpc.newBlockingStub(
            // Create a client channel and register for automatic graceful shutdown.
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    // Build a search request
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);
    AstraSearch.SearchResult response =
        blockingAstraClient.search(
            AstraSearch.SearchRequest.newBuilder()
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(buildQueryFromQueryString("Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregationJson(
                    buildHistogramRequestJSON(chunk1StartTimeMs, chunk1EndTimeMs, 2))
                .build());

    // Validate search response
    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
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
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_DATASET_NAME);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(1);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(1);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(1.0);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(1.0);
    assertThat((String) m.getSource().get("message")).contains("Message1");

    // Test histogram buckets
    InternalDateHistogram dateHistogram =
        (InternalDateHistogram)
            OpenSearchInternalAggregation.fromByteArray(
                response.getInternalAggregations().toByteArray());
    assertThat(dateHistogram.getBuckets().size()).isEqualTo(1);
    assertThat(dateHistogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testAstraGrpcSearchThrowsException() throws IOException {
    // Load test data into chunk manager.
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset, false);
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
            .addService(new AstraLocalQueryService<>(chunkManager, Duration.ofSeconds(3)))
            .build()
            .start());

    // Create a client channel and register for automatic graceful shutdown.
    AstraServiceGrpc.AstraServiceBlockingStub blockingStub =
        AstraServiceGrpc.newBlockingStub(
            // Create a client channel and register for automatic graceful shutdown.
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    // Build a bad search request.
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(
            () ->
                blockingStub.search(
                    AstraSearch.SearchRequest.newBuilder()
                        .setDataset(MessageUtil.TEST_DATASET_NAME)
                        .setQuery(
                            buildQueryFromQueryString(
                                "Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                        .setStartTimeEpochMs(chunk1StartTimeMs)
                        .setEndTimeEpochMs(chunk1EndTimeMs)
                        .setHowMany(0)
                        .setAggregationJson("")
                        .build()));
  }
}
