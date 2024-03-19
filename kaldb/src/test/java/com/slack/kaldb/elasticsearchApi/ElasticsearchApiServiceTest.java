package com.slack.kaldb.elasticsearchApi;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
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
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.SpanUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@SuppressWarnings("UnstableApiUsage")
public class ElasticsearchApiServiceTest {
  private static final String S3_TEST_BUCKET = "test-kaldb-logs";

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
            KaldbConfigUtil.makeIndexerConfig());
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
    KaldbLocalQueryService<LogMessage> searcher =
        new KaldbLocalQueryService<>(chunkManagerUtil.chunkManager, Duration.ofSeconds(3));
    elasticsearchApiService = new ElasticsearchApiService(searcher);
  }

  @AfterEach
  public void tearDown() throws TimeoutException, IOException {
    chunkManagerUtil.close();
    metricsRegistry.close();
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
    KaldbLocalQueryService<LogMessage> slowSearcher =
        spy(new KaldbLocalQueryService<>(chunkManagerUtil.chunkManager, Duration.ofSeconds(5)));

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
    KaldbQueryServiceBase searcher = mock(KaldbQueryServiceBase.class);
    ElasticsearchApiService serviceUnderTest = new ElasticsearchApiService(searcher);

    Instant start = Instant.now();
    Instant end = start.minusSeconds(60);

    when(searcher.getSchema(
            eq(
                KaldbSearch.SchemaRequest.newBuilder()
                    .setDataset("foo")
                    .setStartTimeEpochMs(start.toEpochMilli())
                    .setEndTimeEpochMs(end.toEpochMilli())
                    .build())))
        .thenReturn(KaldbSearch.SchemaResult.newBuilder().build());

    HttpResponse response =
        serviceUnderTest.mapping(
            Optional.of("foo"), Optional.of(start.toEpochMilli()), Optional.of(end.toEpochMilli()));
    verify(searcher)
        .getSchema(
            eq(
                KaldbSearch.SchemaRequest.newBuilder()
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
              KaldbSearch.SchemaRequest request =
                  ((KaldbSearch.SchemaRequest) invocationOnMock.getArguments()[0]);
              assertThat(request.getDataset()).isEqualTo("bar");
              assertThat(request.getStartTimeEpochMs())
                  .isCloseTo(
                      Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli(),
                      Offset.offset(1000L));
              assertThat(request.getEndTimeEpochMs())
                  .isCloseTo(Instant.now().toEpochMilli(), Offset.offset(1000L));
              return KaldbSearch.SchemaResult.newBuilder().build();
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
