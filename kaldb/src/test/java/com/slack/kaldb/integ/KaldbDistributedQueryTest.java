package com.slack.kaldb.integ;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.Kaldb;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class KaldbDistributedQueryTest {
  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static final String TEST_KAFKA_TOPIC_1 = "test-topic-1";
  private static final String KALDB_TEST_CLIENT_1 = "kaldb-test-client1";

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private TestKafkaServer kafkaServer;
  private TestingServer zkServer;

  @Before
  public void setUp() throws Exception {
    kafkaServer = new TestKafkaServer();
    zkServer = new TestingServer();
    zkServer.start();
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
        maxOffsetDelay);
  }

  @Test
  public void testDistributedQueryOneIndexerOneQueryNode() throws Exception {
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

    // Add data to indexer.
    // Query from indexer.
    // Query from query service.
  }
}
