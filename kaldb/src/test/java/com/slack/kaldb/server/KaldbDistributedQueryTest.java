package com.slack.kaldb.server;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.testlib.KaldbGrpcQueryUtil.searchUsingGrpcApi;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.core.instrument.search.MeterNotFoundException;
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

public class KaldbDistributedQueryTest {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbDistributedQueryTest.class);

  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static final String TEST_KAFKA_TOPIC_1 = "test-topic-1";
  private static final String KALDB_TEST_CLIENT_1 = "kaldb-test-client1";

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private TestKafkaServer kafkaServer;
  private TestingServer zkServer;

  @Before
  public void setUp() throws Exception {
    zkServer = new TestingServer();
    zkServer.start();

    kafkaServer = new TestKafkaServer();
  }

  @After
  public void teardown() throws Exception {
    kafkaServer.close();
    zkServer.close();
  }

  public KaldbConfigs.KaldbConfig makeKaldbConfig(
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
        port,
        zkServer.getConnectString(),
        zkPathPrefix,
        nodeRole,
        maxOffsetDelay,
        "api_log");
  }

  @Test
  public void testDistributedQueryOneIndexerOneQueryNode() throws Exception {
    assertThat(kafkaServer.getBroker().isRunning()).isTrue();

    int indexerPort = 10000;
    String indexerPathPrefix = "indexer1";
    // create a kaldb query server and indexer.
    KaldbConfigs.KaldbConfig indexerConfig =
        makeKaldbConfig(
            indexerPort,
            TEST_KAFKA_TOPIC_1,
            0,
            KALDB_TEST_CLIENT_1,
            indexerPathPrefix,
            KaldbConfigs.NodeRole.INDEX,
            1000);

    Kaldb indexer = new Kaldb(indexerConfig);
    indexer.start();

    int queryServicePort = 10001;
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

    // Produce messages to kafka, so the indexer can consume them.
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final int indexedMessagesCount =
        produceMessagesToKafka(kafkaServer.getBroker(), startTime, TEST_KAFKA_TOPIC_1);

    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);
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

    KaldbSearch.SearchResult searchResponse =
        searchUsingGrpcApi(
            "*:*", 0L, 1601547099000L, MessageUtil.TEST_INDEX_NAME, 100, 2, queryServicePort);

    assertThat(searchResponse.getTotalNodes()).isEqualTo(3);
    assertThat(searchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(searchResponse.getTotalCount()).isEqualTo(300);
    assertThat(searchResponse.getHitsCount()).isEqualTo(100);

    // Add data to indexer.
    // Query from indexer.
    // Query from query service.
  }
}
