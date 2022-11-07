package com.slack.kaldb.elasticsearchApi;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@SuppressWarnings("UnstableApiUsage")
public class ElasticsearchApiServiceTest {
  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static final String TEST_KAFKA_PARTITION_ID = "10";
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ElasticsearchApiService elasticsearchApiService;

  private SimpleMeterRegistry metricsRegistry;
  private ChunkManagerUtil<LogMessage> chunkManagerUtil;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        ChunkManagerUtil.makeChunkManagerUtil(
            S3_MOCK_RULE,
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

  @After
  public void tearDown() throws TimeoutException, IOException {
    chunkManagerUtil.close();
    metricsRegistry.close();
  }

  // todo - test mapping
  @Test
  public void testResultsAreReturnedForValidQuery() throws IOException {
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
    addMessagesToChunkManager(messages);

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
  public void testSearchStringWithOneResult() throws IOException {
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
    addMessagesToChunkManager(messages);

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
  public void testSearchStringWithNoResult() throws IOException {
    // add 100 results around now
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
    addMessagesToChunkManager(messages);

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
  public void testResultSizeIsRespected() throws IOException {
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
    addMessagesToChunkManager(messages);

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
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
    addMessagesToChunkManager(messages);
    String postBody =
        Resources.toString(
            Resources.getResource("elasticsearchApi/multisearch_query_10results.ndjson"),
            Charset.defaultCharset());
    KaldbLocalQueryService<LogMessage> slowSearcher =
        spy(new KaldbLocalQueryService<>(chunkManagerUtil.chunkManager, Duration.ofSeconds(3)));

    Set<String> threadNames = ConcurrentHashMap.newKeySet();
    doAnswer(
            invocationOnMock -> {
              threadNames.add(Thread.currentThread().getName());
              return invocationOnMock.callRealMethod();
            })
        .when(slowSearcher)
        .doSearch(any());
    ElasticsearchApiService slowElasticsearchApiService = new ElasticsearchApiService(slowSearcher);
    HttpResponse response = slowElasticsearchApiService.multiSearch(postBody.repeat(100));

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);

    // assert that more than one thread executed our code
    assertThat(threadNames.size()).isGreaterThan(1);

    // ensure we have all 100 results
    assertThat(jsonNode.get("responses").size()).isEqualTo(100);
  }

  @Test
  public void testEmptySearchGrafana7() throws IOException {
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
  public void testEmptySearchGrafana8() throws IOException {
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
    HttpResponse response = elasticsearchApiService.mapping(Optional.of("foo"));

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = new ObjectMapper().readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);

    assertThat(jsonNode.findValue("foo")).isNotNull();
    assertThat(jsonNode.findValue("foo").findValue("@timestamp")).isNotNull();
  }

  private void addMessagesToChunkManager(List<LogMessage> messages) throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();
  }
}
