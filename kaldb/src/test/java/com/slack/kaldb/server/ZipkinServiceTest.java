package com.slack.kaldb.server;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.ZK_PATH_PREFIX;
import static com.slack.kaldb.testlib.KaldbSearchUtils.searchUsingGrpcApi;
import static com.slack.kaldb.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.kaldb.testlib.MessageUtil.TEST_MESSAGE_TYPE;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

public class ZipkinServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(ZipkinServiceTest.class);

  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static final String TEST_KAFKA_TOPIC_1 = "test-topic-1";
  private static final String KALDB_TEST_CLIENT_1 = "kaldb-test-client1";

  private DatasetMetadataStore datasetMetadataStore;
  private ZookeeperMetadataStoreImpl zkMetadataStore;
  private PrometheusMeterRegistry meterRegistry;

  @ClassRule
  public static final S3MockRule S3_MOCK_RULE =
      S3MockRule.builder().withInitialBuckets(TEST_S3_BUCKET).silent().build();

  private TestKafkaServer kafkaServer;
  private TestingServer zkServer;
  private S3Client s3Client;

  @Before
  public void setUp() throws Exception {
    zkServer = new TestingServer();
    kafkaServer = new TestKafkaServer();
    s3Client = S3_MOCK_RULE.createS3ClientV2();

    // We side load a service metadata entry telling it to create an entry with the partitions that
    // we use in test
    meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix(ZK_PATH_PREFIX)
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();
    zkMetadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);
    datasetMetadataStore = new DatasetMetadataStore(zkMetadataStore, true);
    final DatasetPartitionMetadata partition =
        new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0", "1"));
    final List<DatasetPartitionMetadata> partitionConfigs = Collections.singletonList(partition);
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            TEST_DATASET_NAME, "serviceOwner", 1000, partitionConfigs, TEST_DATASET_NAME);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);
  }

  @After
  public void teardown() throws Exception {
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    if (meterRegistry != null) {
      meterRegistry.close();
    }
    if (datasetMetadataStore != null) {
      datasetMetadataStore.close();
    }
    if (zkMetadataStore != null) {
      zkMetadataStore.close();
    }
    if (zkServer != null) {
      zkServer.close();
    }
  }

  public static LogWireMessage makeWireMessageForSpans(
      String id,
      String ts,
      String traceId,
      Optional<String> parentId,
      long durationMs,
      String serviceName,
      String name) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, ts);
    fieldMap.put(LogMessage.ReservedField.TRACE_ID.fieldName, traceId);
    fieldMap.put(LogMessage.ReservedField.SERVICE_NAME.fieldName, serviceName);
    fieldMap.put(LogMessage.ReservedField.NAME.fieldName, name);
    parentId.ifPresent(s -> fieldMap.put(LogMessage.ReservedField.PARENT_ID.fieldName, s));
    fieldMap.put(LogMessage.ReservedField.DURATION_MS.fieldName, durationMs);
    return new LogWireMessage(TEST_DATASET_NAME, TEST_MESSAGE_TYPE, id, fieldMap);
  }

  public static List<LogWireMessage> generateLogWireMessagesForOneTrace(
      Instant time, int count, String traceId) {
    List<LogWireMessage> messages = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      String parentId = null;
      if (i > 1) {
        parentId = String.valueOf(i - 1);
      }
      messages.add(
          makeWireMessageForSpans(
              String.valueOf(i),
              time.plusSeconds(i).toString(),
              traceId,
              Optional.ofNullable(parentId),
              i,
              "service1",
              ("Trace" + i)));
    }
    return messages;
  }

  @Test
  @Ignore // Flakey test, occasionally returns an empty result
  public void testDistributedQueryOneIndexerOneQueryNode() throws Exception {
    assertThat(kafkaServer.getBroker().isRunning()).isTrue();

    LOG.info("Starting query service");
    int queryServicePort = 8887;
    KaldbConfigs.KaldbConfig queryServiceConfig =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + kafkaServer.getBroker().getKafkaPort().get(),
            -1,
            TEST_KAFKA_TOPIC_1,
            0,
            KALDB_TEST_CLIENT_1,
            TEST_S3_BUCKET,
            queryServicePort,
            zkServer.getConnectString(),
            ZK_PATH_PREFIX,
            KaldbConfigs.NodeRole.QUERY,
            1000,
            "api_Log",
            -1,
            100);
    Kaldb queryService = new Kaldb(queryServiceConfig, meterRegistry);
    queryService.start();
    queryService.serviceManager.awaitHealthy(DEFAULT_START_STOP_DURATION);

    int indexerPort = 10000;
    int totalMessagesToIndex = 8;
    LOG.info(
        "Creating indexer service at port {}, topic: {} and partition {}",
        indexerPort,
        TEST_KAFKA_TOPIC_1,
        0);
    // create a kaldb indexer
    KaldbConfigs.KaldbConfig indexerConfig =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + kafkaServer.getBroker().getKafkaPort().get(),
            indexerPort,
            TEST_KAFKA_TOPIC_1,
            0,
            KALDB_TEST_CLIENT_1,
            TEST_S3_BUCKET,
            -1,
            zkServer.getConnectString(),
            ZK_PATH_PREFIX,
            KaldbConfigs.NodeRole.INDEX,
            1000,
            "api_log",
            9003,
            totalMessagesToIndex);

    PrometheusMeterRegistry indexerMeterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    Kaldb indexer = new Kaldb(indexerConfig, s3Client, indexerMeterRegistry);
    indexer.start();
    indexer.serviceManager.awaitHealthy(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    // Produce messages to kafka, so the indexer can consume them.
    final Instant trace1StartTime = Instant.now().minus(20, ChronoUnit.MINUTES);
    List<LogWireMessage> messages =
        new ArrayList<>(generateLogWireMessagesForOneTrace(trace1StartTime, 2, "1"));

    final Instant trace2StartTime = Instant.now().minus(10, ChronoUnit.MINUTES);
    messages.addAll(generateLogWireMessagesForOneTrace(trace2StartTime, 5, "2"));

    final Instant trace3StartTime = Instant.now().minus(5, ChronoUnit.MINUTES);
    messages.addAll(generateLogWireMessagesForOneTrace(trace3StartTime, 1, "3"));

    List<LogMessage> logMessages =
        messages.stream().map(LogMessage::fromWireMessage).collect(Collectors.toList());

    final int indexedMessagesCount =
        produceMessagesToKafka(kafkaServer.getBroker(), TEST_KAFKA_TOPIC_1, 0, logMessages);
    assertThat(totalMessagesToIndex).isEqualTo(indexedMessagesCount);

    await()
        .until(
            () ->
                getCount(MESSAGES_RECEIVED_COUNTER, indexerMeterRegistry) == indexedMessagesCount);

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, indexerMeterRegistry) == 1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, indexerMeterRegistry)).isZero();

    // Query from the grpc search service
    KaldbSearch.SearchResult queryServiceSearchResponse =
        searchUsingGrpcApi("*:*", queryServicePort, 0, Instant.now().toEpochMilli(), "365d");

    assertThat(queryServiceSearchResponse.getTotalNodes()).isEqualTo(1);
    assertThat(queryServiceSearchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(queryServiceSearchResponse.getHitsCount()).isEqualTo(indexedMessagesCount);

    // Query from the zipkin search service
    String endpoint = "http://127.0.0.1:" + queryServicePort;
    WebClient webClient = WebClient.of(endpoint);
    AggregatedHttpResponse response = webClient.get("/api/v2/trace/1").aggregate().join();
    String body = response.content(StandardCharsets.UTF_8);
    assertThat(response.status().code()).isEqualTo(200);
    String expectedTrace =
        String.format(
            "[{\n"
                + "  \"traceId\": \"1\",\n"
                + "  \"parentId\": \"1\",\n"
                + "  \"id\": \"localhost:100:1\",\n"
                + "  \"kind\": \"SPAN_KIND_UNSPECIFIED\",\n"
                + "  \"name\": \"Trace2\",\n"
                + "  \"timestamp\": \"%d\",\n"
                + "  \"duration\": \"2\",\n"
                + "  \"remoteEndpoint\": {\n"
                + "    \"serviceName\": \"testDataSet\",\n"
                + "    \"ipv4\": \"\",\n"
                + "    \"ipv6\": \"\",\n"
                + "    \"port\": 0\n"
                + "  },\n"
                + "  \"annotations\": [],\n"
                + "  \"tags\": {\n"
                + "    \"hostname\": \"localhost\"\n"
                + "  },\n"
                + "  \"debug\": false,\n"
                + "  \"shared\": false\n"
                + "},{\n"
                + "  \"traceId\": \"1\",\n"
                + "  \"parentId\": \"\",\n"
                + "  \"id\": \"localhost:100:0\",\n"
                + "  \"kind\": \"SPAN_KIND_UNSPECIFIED\",\n"
                + "  \"name\": \"Trace1\",\n"
                + "  \"timestamp\": \"%d\",\n"
                + "  \"duration\": \"1\",\n"
                + "  \"remoteEndpoint\": {\n"
                + "    \"serviceName\": \"testDataSet\",\n"
                + "    \"ipv4\": \"\",\n"
                + "    \"ipv6\": \"\",\n"
                + "    \"port\": 0\n"
                + "  },\n"
                + "  \"annotations\": [],\n"
                + "  \"tags\": {\n"
                + "    \"hostname\": \"localhost\"\n"
                + "  },\n"
                + "  \"debug\": false,\n"
                + "  \"shared\": false\n"
                + "}]",
            ZipkinService.convertToMicroSeconds(trace1StartTime.plusSeconds(2)),
            ZipkinService.convertToMicroSeconds(trace1StartTime.plusSeconds(1)));
    assertThat(body).isEqualTo(expectedTrace);

    String params =
        String.format(
            "?startTimeEpochMs=%d&endTimeEpochMs=%d",
            trace1StartTime.minus(10, ChronoUnit.SECONDS).toEpochMilli(),
            trace1StartTime.plus(5, ChronoUnit.SECONDS).toEpochMilli());
    response = webClient.get("/api/v2/trace/1" + params).aggregate().join();
    body = response.content(StandardCharsets.UTF_8);
    assertThat(response.status().code()).isEqualTo(200);
    assertThat(body).isEqualTo(expectedTrace);

    params =
        String.format(
            "?startTimeEpochMs=%d&endTimeEpochMs=%d",
            trace1StartTime.plus(0, ChronoUnit.SECONDS).toEpochMilli(),
            trace1StartTime.plus(2, ChronoUnit.SECONDS).toEpochMilli());
    response = webClient.get("/api/v2/trace/1" + params).aggregate().join();
    body = response.content(StandardCharsets.UTF_8);
    assertThat(response.status().code()).isEqualTo(200);
    assertThat(body).isEqualTo(expectedTrace);

    params =
        String.format(
            "?startTimeEpochMs=%d&endTimeEpochMs=%d",
            trace1StartTime.plus(1, ChronoUnit.SECONDS).toEpochMilli(),
            trace1StartTime.plus(2, ChronoUnit.SECONDS).toEpochMilli());
    response = webClient.get("/api/v2/trace/1" + params).aggregate().join();
    body = response.content(StandardCharsets.UTF_8);
    assertThat(response.status().code()).isEqualTo(200);
    assertThat(body).isEqualTo(expectedTrace);

    params = String.format("?maxSpans=%d", 1);
    response = webClient.get("/api/v2/trace/1" + params).aggregate().join();
    body = response.content(StandardCharsets.UTF_8);
    assertThat(response.status().code()).isEqualTo(200);
    expectedTrace =
        String.format(
            "[{\n"
                + "  \"traceId\": \"1\",\n"
                + "  \"parentId\": \"1\",\n"
                + "  \"id\": \"localhost:100:1\",\n"
                + "  \"kind\": \"SPAN_KIND_UNSPECIFIED\",\n"
                + "  \"name\": \"Trace2\",\n"
                + "  \"timestamp\": \"%d\",\n"
                + "  \"duration\": \"2\",\n"
                + "  \"remoteEndpoint\": {\n"
                + "    \"serviceName\": \"testDataSet\",\n"
                + "    \"ipv4\": \"\",\n"
                + "    \"ipv6\": \"\",\n"
                + "    \"port\": 0\n"
                + "  },\n"
                + "  \"annotations\": [],\n"
                + "  \"tags\": {\n"
                + "    \"hostname\": \"localhost\"\n"
                + "  },\n"
                + "  \"debug\": false,\n"
                + "  \"shared\": false\n"
                + "}]",
            ZipkinService.convertToMicroSeconds(trace1StartTime.plusSeconds(2)));
    assertThat(body).isEqualTo(expectedTrace);

    params =
        String.format(
            "?startTimeEpochMs=%d&endTimeEpochMs=%d",
            trace1StartTime.plus(2, ChronoUnit.SECONDS).toEpochMilli(),
            trace1StartTime.plus(3, ChronoUnit.SECONDS).toEpochMilli());
    response = webClient.get("/api/v2/trace/1" + params).aggregate().join();
    body = response.content(StandardCharsets.UTF_8);
    assertThat(response.status().code()).isEqualTo(200);
    assertThat(body).isEqualTo(expectedTrace);

    params =
        String.format(
            "?startTimeEpochMs=%d&endTimeEpochMs=%d",
            trace1StartTime.minus(10, ChronoUnit.SECONDS).toEpochMilli(),
            trace1StartTime.minus(1, ChronoUnit.SECONDS).toEpochMilli());
    response = webClient.get("/api/v2/trace/1" + params).aggregate().join();
    body = response.content(StandardCharsets.UTF_8);
    assertThat(response.status().code()).isEqualTo(200);
    expectedTrace = "[]";
    assertThat(body).isEqualTo(expectedTrace);

    response = webClient.get("/api/v2/trace/3").aggregate().join();
    body = response.content(StandardCharsets.UTF_8);
    assertThat(response.status().code()).isEqualTo(200);
    expectedTrace =
        String.format(
            "[{\n"
                + "  \"traceId\": \"3\",\n"
                + "  \"parentId\": \"\",\n"
                + "  \"id\": \"localhost:100:7\",\n"
                + "  \"kind\": \"SPAN_KIND_UNSPECIFIED\",\n"
                + "  \"name\": \"Trace1\",\n"
                + "  \"timestamp\": \"%d\",\n"
                + "  \"duration\": \"1\",\n"
                + "  \"remoteEndpoint\": {\n"
                + "    \"serviceName\": \"testDataSet\",\n"
                + "    \"ipv4\": \"\",\n"
                + "    \"ipv6\": \"\",\n"
                + "    \"port\": 0\n"
                + "  },\n"
                + "  \"annotations\": [],\n"
                + "  \"tags\": {\n"
                + "    \"hostname\": \"localhost\"\n"
                + "  },\n"
                + "  \"debug\": false,\n"
                + "  \"shared\": false\n"
                + "}]",
            ZipkinService.convertToMicroSeconds(trace3StartTime.plusSeconds(1)));
    assertThat(body).isEqualTo(expectedTrace);

    response = webClient.get("/api/v2/trace/4").aggregate().join();
    body = response.content(StandardCharsets.UTF_8);
    assertThat(response.status().code()).isEqualTo(200);
    expectedTrace = "[]";
    assertThat(body).isEqualTo(expectedTrace);

    // Shutdown
    LOG.info("Shutting down query service.");
    queryService.shutdown();
    LOG.info("Shutting down indexer.");
    indexer.shutdown();
  }
}
