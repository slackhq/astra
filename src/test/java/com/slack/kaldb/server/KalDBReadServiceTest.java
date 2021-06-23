package com.slack.kaldb.server;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.LocalDateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class KalDBReadServiceTest {

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static SimpleMeterRegistry indexerMetricsRegistry;
  private static ChunkManagerUtil<LogMessage> chunkManagerUtil;

  private static KaldbServiceGrpc.KaldbServiceBlockingStub readServiceStub;
  private static Server indexingServer;
  private static Server readServer;

  private static final String TEST_KAFKA_TOPIC = "test-topic";

  // Kafka producer creates only a partition 0 on first message. So, set the partition to 0 always.
  private static final int TEST_KAFKA_PARTITION = 0;

  private static final String KALDB_TEST_CLIENT = "kaldb-test-client";
  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static TestKafkaServer kafkaServer;

  @BeforeClass
  public static void initialize() throws Exception {
    kafkaServer = new TestKafkaServer();
    EphemeralKafkaBroker broker = kafkaServer.getBroker();
    assertThat(broker.isRunning()).isTrue();

    KaldbConfigs.KaldbConfig kaldbConfig =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + broker.getKafkaPort().get(),
            8080,
            TEST_KAFKA_TOPIC,
            TEST_KAFKA_PARTITION,
            KALDB_TEST_CLIENT,
            TEST_S3_BUCKET,
            8081);
    KaldbConfig.initFromConfigObject(kaldbConfig);

    indexingServer = newIndexingServer(kaldbConfig);
    indexingServer.start().join();

    // Produce messages to kafka, so the indexer can consume them.
    final LocalDateTime startTime = LocalDateTime.of(2020, 10, 1, 10, 10, 0);
    produceMessagesToKafka(broker, startTime);
    Thread.sleep(1000); // Wait for consumer to finish consumption and roll over chunk.
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, indexerMetricsRegistry)).isEqualTo(100);

    readServer = newReadServer(kaldbConfig);
    readServer.start().join();

    // We want to query the indexing server
    KalDBReadService.servers = "gproto+http://127.0.0.1:8080/";
    readServiceStub =
        Clients.newClient(
            "gproto+http://127.0.0.1:8081/", KaldbServiceGrpc.KaldbServiceBlockingStub.class);
  }

  private static Server newIndexingServer(KaldbConfigs.KaldbConfig kaldbConfig)
      throws InterruptedException {
    indexerMetricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        new ChunkManagerUtil<>(S3_MOCK_RULE, indexerMetricsRegistry, 10 * 1024 * 1024 * 1024L, 100);
    KaldbIndexer indexer =
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            KaldbIndexer.dataTransformerMap.get("api_log"),
            indexerMetricsRegistry);
    indexer.start();
    Thread.sleep(1000); // Wait for consumer start.

    KaldbLocalSearcher<LogMessage> service = new KaldbLocalSearcher<>(indexer.getChunkManager());

    return Server.builder()
        .http(kaldbConfig.getIndexerConfig().getServerPort())
        .verboseResponses(true)
        .service(GrpcService.builder().addService(service).build())
        .build();
  }

  private static Server newReadServer(KaldbConfigs.KaldbConfig kaldbConfig) {
    KalDBReadService service = new KalDBReadService();
    return Server.builder()
        .http(kaldbConfig.getReadConfig().getServerPort())
        .verboseResponses(true)
        .service(GrpcService.builder().addService(service).build())
        .build();
  }

  @AfterClass
  public static void shutdownServer() throws Exception {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    if (indexerMetricsRegistry != null) {
      indexerMetricsRegistry.close();
    }
    if (indexingServer != null) {
      indexingServer.stop().join();
    }
    if (readServer != null) {
      readServer.stop().join();
    }
  }

  @Test
  public void testSearch() {
    KaldbSearch.SearchResult searchResponse =
        readServiceStub.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("*:*")
                .setStartTimeEpochMs(0L)
                .setEndTimeEpochMs(1601547099000L)
                .setHowMany(100)
                .setBucketCount(2)
                .build());

    assertThat(searchResponse.getHitsCount()).isEqualTo(100);
  }
}
