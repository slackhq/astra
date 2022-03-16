package com.slack.kaldb.server;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.testlib.KaldbGrpcQueryUtil.runHealthCheckOnPort;
import static com.slack.kaldb.testlib.KaldbGrpcQueryUtil.searchUsingGrpcApi;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.curator.test.TestingServer;
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

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private TestKafkaServer kafkaServer;
  private TestingServer zkServer;
  private S3Client s3Client;

  @Before
  public void setUp() throws Exception {
    zkServer = new TestingServer(2181);
    kafkaServer = new TestKafkaServer(9092);
    s3Client = S3_MOCK_RULE.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(TEST_S3_BUCKET).build());
  }

  @After
  public void teardown() throws Exception {
    kafkaServer.close();
    zkServer.close();
  }

  private KaldbConfigs.KaldbConfig makeKaldbConfig(
      int port,
      String kafkaTopic,
      int kafkaPartition,
      String clientName,
      String zkPathPrefix,
      KaldbConfigs.NodeRole nodeRole,
      int maxOffsetDelay) {
    return KaldbConfigUtil.makeKaldbConfig(
        "localhost:" + kafkaServer.getBroker().getKafkaPort().get(),
        port,
        kafkaTopic,
        kafkaPartition,
        clientName,
        TEST_S3_BUCKET,
        port + 1,
        zkServer.getConnectString(),
        zkPathPrefix,
        nodeRole,
        maxOffsetDelay,
        "api_log");
  }

  private Kaldb makeIndexerAndIndexMessages(
      int indexerPort,
      String kafkaTopic,
      int kafkaPartition,
      String kafkaClient,
      String indexerPathPrefix,
      int indexerCount)
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
            kafkaTopic,
            kafkaPartition,
            kafkaClient,
            indexerPathPrefix,
            KaldbConfigs.NodeRole.INDEX,
            1000);

    Kaldb indexer = new Kaldb(indexerConfig, s3Client);
    indexer.start();
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == indexerCount);

    // Produce messages to kafka, so the indexer can consume them.
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final int indexedMessagesCount =
        produceMessagesToKafka(kafkaServer.getBroker(), startTime, kafkaTopic, 0);

    await()
        .until(
            () -> {
              try {
                double count = getCount(MESSAGES_RECEIVED_COUNTER, indexer.prometheusMeterRegistry);
                LOG.debug("Registry1 current_count={} total_count={}", count, indexedMessagesCount);
                return count == indexedMessagesCount;
              } catch (MeterNotFoundException e) {
                return false;
              }
            });

    await()
        .until(
            () ->
                getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, indexer.prometheusMeterRegistry)
                    == 1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, indexer.prometheusMeterRegistry))
        .isZero();

    return indexer;
  }

  @Test
  public void testDistributedQueryOneIndexerOneQueryNode() throws Exception {
    assertThat(kafkaServer.getBroker().isRunning()).isTrue();
    String indexerPathPrefix = "indexer1";

    LOG.info("Starting query service");
    int queryServicePort = 11000;
    KaldbConfigs.KaldbConfig queryServiceConfig =
        makeKaldbConfig(
            queryServicePort,
            TEST_KAFKA_TOPIC_1,
            0,
            KALDB_TEST_CLIENT_1,
            indexerPathPrefix,
            KaldbConfigs.NodeRole.QUERY,
            1000);
    Kaldb queryService = new Kaldb(queryServiceConfig);
    queryService.start();
    queryService.serviceManager.awaitHealthy();

    LOG.info("Starting indexer service");
    int indexerPort = 10000;

    Kaldb indexer =
        makeIndexerAndIndexMessages(
            indexerPort, TEST_KAFKA_TOPIC_1, 0, KALDB_TEST_CLIENT_1, indexerPathPrefix, 1);
    indexer.serviceManager.awaitHealthy();

    KaldbSearch.SearchResult indexerSearchResponse =
        searchUsingGrpcApi(
            "*:*", 0L, 1601547099000L, MessageUtil.TEST_INDEX_NAME, 100, 2, indexerPort);
    assertThat(indexerSearchResponse.getTotalNodes()).isEqualTo(1);
    assertThat(indexerSearchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(indexerSearchResponse.getTotalCount()).isEqualTo(100);
    assertThat(indexerSearchResponse.getHitsCount()).isEqualTo(100);

    // Query from query service.
    KaldbSearch.SearchResult queryServiceSearchResponse =
        searchUsingGrpcApi(
            "*:*", 0L, 1601547099000L, MessageUtil.TEST_INDEX_NAME, 100, 2, queryServicePort + 1);

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
    KaldbConfigs.KaldbConfig kaldbConfig =
        KaldbConfig.fromYamlConfig(Files.readString(Path.of("../config/config.yaml")));
    Kaldb kaldb = new Kaldb(kaldbConfig);
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

    // shutdown
    kaldb.shutdown();
  }

  // TODO: Add a unit test with 2 indexers and a query service.
  // TODO: Add a unit test where a shard times out.
}
