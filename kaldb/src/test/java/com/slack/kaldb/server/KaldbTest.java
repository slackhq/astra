package com.slack.kaldb.server;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.ZK_PATH_PREFIX;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.linecorp.armeria.client.grpc.GrpcClients;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;
import org.apache.curator.test.TestingServer;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class KaldbTest {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbTest.class);

  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static final String TEST_KAFKA_TOPIC_1 = "test-topic-1";
  private static final String KALDB_TEST_CLIENT_1 = "kaldb-test-client1";
  private static final String KALDB_TEST_CLIENT_2 = "kaldb-test-client2";

  private DatasetMetadataStore datasetMetadataStore;
  private ZookeeperMetadataStoreImpl zkMetadataStore;
  private PrometheusMeterRegistry meterRegistry;

  public static KaldbSearch.SearchResult searchUsingGrpcApi(
      String queryString, int port, long startTime, long endTime) {
    KaldbServiceGrpc.KaldbServiceBlockingStub kaldbService =
        GrpcClients.builder(uri(port))
            .build(KaldbServiceGrpc.KaldbServiceBlockingStub.class)
            .withCompression("gzip");

    return kaldbService.search(
        KaldbSearch.SearchRequest.newBuilder()
            .setDataset(MessageUtil.TEST_DATASET_NAME)
            .setQueryString(queryString)
            .setStartTimeEpochMs(startTime)
            .setEndTimeEpochMs(endTime)
            .setHowMany(100)
            .setBucketCount(2)
            .build());
  }

  private static String uri(int port) {
    return "gproto+http://127.0.0.1:" + port + '/';
  }

  private static String getHealthCheckResponse(String url) {
    try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
      HttpGet httpGet = new HttpGet(url);
      try (CloseableHttpResponse httpResponse = httpclient.execute(httpGet)) {
        HttpEntity entity = httpResponse.getEntity();

        String response = EntityUtils.toString(entity);
        EntityUtils.consume(entity);
        return response;
      }
    } catch (IOException e) {
      return null;
    }
  }

  private static String getHealthCheckResponse(int port) {
    String url = String.format("http://localhost:%s/health", port);
    return getHealthCheckResponse(url);
  }

  private static boolean runHealthCheckOnPort(KaldbConfigs.ServerConfig serverConfig)
      throws JsonProcessingException {
    final ObjectMapper om = new ObjectMapper();
    final String response = getHealthCheckResponse(serverConfig.getServerPort());
    HashMap<String, Object> map = om.readValue(response, HashMap.class);

    LOG.info(String.format("Response from healthcheck - '%s'", response));
    return (boolean) map.get("healthy");
  }

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private TestKafkaServer kafkaServer;
  private TestingServer zkServer;
  private S3Client s3Client;

  @Before
  public void setUp() throws Exception {
    zkServer = new TestingServer();
    kafkaServer = new TestKafkaServer();
    s3Client = S3_MOCK_RULE.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(TEST_S3_BUCKET).build());

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
        new DatasetMetadata(MessageUtil.TEST_DATASET_NAME, "serviceOwner", 1000, partitionConfigs);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);
  }

  @After
  public void teardown() throws Exception {
    kafkaServer.close();
    meterRegistry.close();
    datasetMetadataStore.close();
    zkMetadataStore.close();
    zkServer.close();
  }

  private KaldbConfigs.KaldbConfig makeKaldbConfig(
      int indexPort,
      int queryPort,
      String kafkaTopic,
      int kafkaPartition,
      String clientName,
      String zkPathPrefix,
      KaldbConfigs.NodeRole nodeRole,
      int maxOffsetDelay,
      int recoveryPort) {
    return KaldbConfigUtil.makeKaldbConfig(
        "localhost:" + kafkaServer.getBroker().getKafkaPort().get(),
        indexPort,
        kafkaTopic,
        kafkaPartition,
        clientName,
        TEST_S3_BUCKET,
        queryPort,
        zkServer.getConnectString(),
        zkPathPrefix,
        nodeRole,
        maxOffsetDelay,
        "api_log",
        recoveryPort);
  }

  private Kaldb makeIndexerAndIndexMessages(
      int indexerPort,
      String kafkaTopic,
      int kafkaPartition,
      String kafkaClient,
      String indexerPathPrefix,
      int indexerCount,
      Instant indexedMessagesStartTime)
      throws Exception {
    LOG.info(
        "Creating indexer service at port {}, topic: {} and partition {}",
        indexerPort,
        kafkaTopic,
        kafkaPartition);
    // create a kaldb query server and indexer.
    KaldbConfigs.KaldbConfig indexerConfig =
        makeKaldbConfig(
            indexerPort,
            -1,
            kafkaTopic,
            kafkaPartition,
            kafkaClient,
            indexerPathPrefix,
            KaldbConfigs.NodeRole.INDEX,
            1000,
            9003);

    PrometheusMeterRegistry indexerMeterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    Kaldb indexer = new Kaldb(indexerConfig, s3Client, indexerMeterRegistry);
    indexer.start();
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == indexerCount);

    // Produce messages to kafka, so the indexer can consume them.
    final int indexedMessagesCount =
        produceMessagesToKafka(
            kafkaServer.getBroker(), indexedMessagesStartTime, kafkaTopic, kafkaPartition);

    await()
        .until(
            () -> {
              try {
                double count = getCount(MESSAGES_RECEIVED_COUNTER, indexerMeterRegistry);
                LOG.debug("Registry1 current_count={} total_count={}", count, indexedMessagesCount);
                return count == indexedMessagesCount;
              } catch (MeterNotFoundException e) {
                return false;
              }
            });

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, indexerMeterRegistry) == 1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, indexerMeterRegistry)).isZero();

    return indexer;
  }

  @Test
  public void testDistributedQueryOneIndexerOneQueryNode() throws Exception {
    assertThat(kafkaServer.getBroker().isRunning()).isTrue();

    LOG.info("Starting query service");
    int queryServicePort = 8887;
    KaldbConfigs.KaldbConfig queryServiceConfig =
        makeKaldbConfig(
            -1,
            queryServicePort,
            TEST_KAFKA_TOPIC_1,
            0,
            KALDB_TEST_CLIENT_1,
            ZK_PATH_PREFIX,
            KaldbConfigs.NodeRole.QUERY,
            1000,
            -1);
    Kaldb queryService = new Kaldb(queryServiceConfig, meterRegistry);
    queryService.start();
    queryService.serviceManager.awaitHealthy(DEFAULT_START_STOP_DURATION);

    LOG.info("Starting indexer service");
    int indexerPort = 10000;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    Kaldb indexer =
        makeIndexerAndIndexMessages(
            indexerPort, TEST_KAFKA_TOPIC_1, 0, KALDB_TEST_CLIENT_1, ZK_PATH_PREFIX, 1, startTime);
    indexer.serviceManager.awaitHealthy(DEFAULT_START_STOP_DURATION);

    KaldbSearch.SearchResult indexerSearchResponse =
        searchUsingGrpcApi("*:*", indexerPort, 0, 1601547099000L);
    assertThat(indexerSearchResponse.getTotalNodes()).isEqualTo(1);
    assertThat(indexerSearchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(indexerSearchResponse.getTotalCount()).isEqualTo(100);
    assertThat(indexerSearchResponse.getHitsCount()).isEqualTo(100);
    Thread.sleep(2000);

    // Query from query service.
    KaldbSearch.SearchResult queryServiceSearchResponse =
        searchUsingGrpcApi("*:*", queryServicePort, 0, 1601547099000L);

    assertThat(queryServiceSearchResponse.getTotalNodes()).isEqualTo(1);
    assertThat(queryServiceSearchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(queryServiceSearchResponse.getTotalCount()).isEqualTo(100);
    assertThat(queryServiceSearchResponse.getHitsCount()).isEqualTo(100);

    // Shutdown
    LOG.info("Shutting down query service.");
    queryService.shutdown();
    LOG.info("Shutting down indexer.");
    indexer.shutdown();
  }

  @Test
  public void testBootAllComponentsStartSuccessfullyFromConfig() throws Exception {
    Map<String, String> values =
        ImmutableMap.of(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:" + kafkaServer.getBroker().getKafkaPort().get(),
            "KALDB_ZK_CONNECTION_STRING", "localhost:" + zkServer.getPort());
    StringSubstitutor substitute = new StringSubstitutor(s -> values.getOrDefault(s, null));

    KaldbConfigs.KaldbConfig kaldbConfig =
        KaldbConfig.fromYamlConfig(
            substitute.replace(Files.readString(Path.of("../config/config.yaml"))));

    Kaldb kaldb = new Kaldb(kaldbConfig, meterRegistry);
    LOG.info("Starting kalDb with the resolved configs: {}", kaldbConfig);
    kaldb.start();

    kaldb.serviceManager.awaitHealthy();
    assertThat(runHealthCheckOnPort(kaldbConfig.getIndexerConfig().getServerConfig()))
        .isEqualTo(true);
    assertThat(runHealthCheckOnPort(kaldbConfig.getQueryConfig().getServerConfig()))
        .isEqualTo(true);
    assertThat(runHealthCheckOnPort(kaldbConfig.getCacheConfig().getServerConfig()))
        .isEqualTo(true);
    assertThat(runHealthCheckOnPort(kaldbConfig.getRecoveryConfig().getServerConfig()))
        .isEqualTo(true);
    assertThat(runHealthCheckOnPort(kaldbConfig.getManagerConfig().getServerConfig()))
        .isEqualTo(true);
    assertThat(runHealthCheckOnPort(kaldbConfig.getPreprocessorConfig().getServerConfig()))
        .isEqualTo(true);

    // shutdown
    kaldb.shutdown();
  }

  @Test
  public void testTwoIndexersAndOneQueryService() throws Exception {
    assertThat(kafkaServer.getBroker().isRunning()).isTrue();

    LOG.info("Starting query service");
    int queryServicePort = 8888;
    KaldbConfigs.KaldbConfig queryServiceConfig =
        makeKaldbConfig(
            -1,
            queryServicePort,
            TEST_KAFKA_TOPIC_1,
            0,
            KALDB_TEST_CLIENT_1,
            ZK_PATH_PREFIX,
            KaldbConfigs.NodeRole.QUERY,
            1000,
            -1);
    Kaldb queryService = new Kaldb(queryServiceConfig, meterRegistry);
    queryService.start();
    queryService.serviceManager.awaitHealthy(DEFAULT_START_STOP_DURATION);

    LOG.info("Starting indexer service 1");
    int indexerPort = 10000;
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    Kaldb indexer1 =
        makeIndexerAndIndexMessages(
            indexerPort, TEST_KAFKA_TOPIC_1, 0, KALDB_TEST_CLIENT_1, ZK_PATH_PREFIX, 1, startTime);
    indexer1.serviceManager.awaitHealthy(DEFAULT_START_STOP_DURATION);

    LOG.info("Starting indexer service 2");
    int indexerPort2 = 11000;
    final Instant startTime2 =
        LocalDateTime.of(2021, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    Kaldb indexer2 =
        makeIndexerAndIndexMessages(
            indexerPort2,
            TEST_KAFKA_TOPIC_1,
            1,
            KALDB_TEST_CLIENT_2,
            ZK_PATH_PREFIX,
            2,
            startTime2);
    indexer2.serviceManager.awaitHealthy(DEFAULT_START_STOP_DURATION);

    KaldbSearch.SearchResult indexerSearchResponse =
        searchUsingGrpcApi("*:*", indexerPort, 0L, 1601547099000L);
    assertThat(indexerSearchResponse.getTotalNodes()).isEqualTo(1);
    assertThat(indexerSearchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(indexerSearchResponse.getTotalCount()).isEqualTo(100);
    assertThat(indexerSearchResponse.getHitsCount()).isEqualTo(100);

    KaldbSearch.SearchResult indexer2SearchResponse =
        searchUsingGrpcApi("*:*", indexerPort2, 1633083000000L, 1633083099000L);
    assertThat(indexer2SearchResponse.getTotalNodes()).isEqualTo(1);
    assertThat(indexer2SearchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(indexer2SearchResponse.getTotalCount()).isEqualTo(100);
    assertThat(indexer2SearchResponse.getHitsCount()).isEqualTo(100);

    // Query from query service.
    // When we query with a limited timeline (0,1601547099000) we will only query index 1
    KaldbSearch.SearchResult queryServiceSearchResponse =
        searchUsingGrpcApi("*:*", queryServicePort, 0, 1601547099000L);

    assertThat(queryServiceSearchResponse.getTotalNodes()).isEqualTo(1);
    assertThat(queryServiceSearchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(queryServiceSearchResponse.getTotalCount()).isEqualTo(100);
    assertThat(queryServiceSearchResponse.getHitsCount()).isEqualTo(100);

    // When we query with a limited timeline (0,MAX_VALUE) we will only query index 1 AND indexer 2
    queryServiceSearchResponse = searchUsingGrpcApi("*:*", queryServicePort, 0, Long.MAX_VALUE);

    assertThat(queryServiceSearchResponse.getTotalNodes()).isEqualTo(2);
    assertThat(queryServiceSearchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(queryServiceSearchResponse.getTotalCount()).isEqualTo(200);
    assertThat(queryServiceSearchResponse.getHitsCount()).isEqualTo(100);

    // Query from query service.
    KaldbSearch.SearchResult queryServiceSearchResponse2 =
        searchUsingGrpcApi("Message100", queryServicePort, 0, Long.MAX_VALUE);

    assertThat(queryServiceSearchResponse2.getTotalNodes()).isEqualTo(2);
    assertThat(queryServiceSearchResponse2.getFailedNodes()).isEqualTo(0);
    assertThat(queryServiceSearchResponse2.getTotalCount()).isEqualTo(2);
    assertThat(queryServiceSearchResponse2.getHitsCount()).isEqualTo(2);

    // Shutdown
    LOG.info("Shutting down query service.");
    queryService.shutdown();
    LOG.info("Shutting down indexers.");
    indexer1.shutdown();
    indexer2.shutdown();
  }

  // TODO: Add a test where a shard times out.
}
