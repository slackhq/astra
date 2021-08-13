package com.slack.kaldb.elasticsearchApi;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.MoreExecutors;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.ChunkManager;
import com.slack.kaldb.chunk.ChunkRollOverStrategyImpl;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@SuppressWarnings("UnstableApiUsage")
public class ElasticsearchApiServiceTest {
  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private S3Client s3Client;
  private ChunkManager<LogMessage> chunkManager;
  private ElasticsearchApiService elasticsearchApiService;

  private static final String S3_TEST_BUCKET = "test-kaldb-logs";
  private static final String CHUNK_DATA_PREFIX = "testData";

  @Before
  public void setUp() throws IOException, TimeoutException {
    Tracing.newBuilder().build();
    KaldbConfigUtil.initEmptyIndexerConfig();

    // create an S3 client and a bucket for test
    s3Client = S3_MOCK_RULE.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_TEST_BUCKET).build());

    S3BlobFs s3BlobFs = new S3BlobFs();
    s3BlobFs.init(s3Client);

    chunkManager =
        new ChunkManager<>(
            CHUNK_DATA_PREFIX,
            temporaryFolder.newFolder().getAbsolutePath(),
            new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 1000000L),
            new SimpleMeterRegistry(),
            s3BlobFs,
            S3_TEST_BUCKET,
            MoreExecutors.newDirectExecutorService(),
            3000);
    chunkManager.startAsync();
    chunkManager.awaitRunning(15, TimeUnit.SECONDS);

    KaldbLocalQueryService<LogMessage> searcher = new KaldbLocalQueryService<>(chunkManager);
    elasticsearchApiService = new ElasticsearchApiService(searcher);
  }

  @After
  public void tearDown() throws TimeoutException {
    chunkManager.stopAsync();
    chunkManager.awaitTerminated(15, TimeUnit.SECONDS);
    s3Client.close();
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
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    chunkManager.getActiveChunk().commit();
  }
}
