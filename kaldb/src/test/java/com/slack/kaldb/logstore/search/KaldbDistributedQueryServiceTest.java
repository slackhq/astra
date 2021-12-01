package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.util.EventLoopGroups;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.server.KaldbIndexer;
import com.slack.kaldb.server.KaldbTimeoutLocalQueryService;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.writer.LogMessageTransformer;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.kaldb.writer.kafka.KaldbKafkaWriter;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class KaldbDistributedQueryServiceTest {

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static SimpleMeterRegistry indexerMetricsRegistry1 = new SimpleMeterRegistry();
  private static SimpleMeterRegistry indexerMetricsRegistry2 = new SimpleMeterRegistry();

  private static KaldbServiceGrpc.KaldbServiceBlockingStub queryServiceStub;
  private static Server indexingServer1;
  private static Server indexingServer2;
  private static Server queryServer;

  private static final String TEST_KAFKA_TOPIC_1 = "test-topic-1";
  private static final String TEST_KAFKA_TOPIC_2 = "test-topic-2";

  // Kafka producer creates only a partition 0 on first message. So, set the partition to 0 always.
  private static final int TEST_KAFKA_PARTITION_1 = 0;
  private static final int TEST_KAFKA_PARTITION_2 = 0;

  private static final String KALDB_TEST_CLIENT = "kaldb-test-client";
  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static TestKafkaServer kafkaServer;

  @BeforeClass
  // TODO: This test is very similar to KaldbIndexerTest - explore a TestRule based setup
  public static void initialize() throws Exception {
    Tracing.newBuilder().build();
    kafkaServer = new TestKafkaServer();

    EphemeralKafkaBroker broker = kafkaServer.getBroker();
    assertThat(broker.isRunning()).isTrue();

    KaldbConfigs.KaldbConfig kaldbConfig1 =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + broker.getKafkaPort().get(),
            0,
            TEST_KAFKA_TOPIC_1,
            TEST_KAFKA_PARTITION_1,
            KALDB_TEST_CLIENT,
            TEST_S3_BUCKET,
            8081,
            "",
            "");

    // Needed to set refresh/commit interval etc
    // Will be same for both indexing servers
    KaldbConfig.initFromConfigObject(kaldbConfig1);

    SearchContext searchContext1 = new SearchContext("localhost", 10000);
    ChunkManagerUtil<LogMessage> chunkManagerUtil1 =
        new ChunkManagerUtil<>(
            S3_MOCK_RULE, indexerMetricsRegistry1, 10 * 1024 * 1024 * 1024L, 100, searchContext1);
    indexingServer1 =
        newIndexingServer(chunkManagerUtil1, kaldbConfig1, indexerMetricsRegistry1, 0);
    indexingServer1.start().get(10, TimeUnit.SECONDS);

    KaldbConfigs.KaldbConfig kaldbConfig2 =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + broker.getKafkaPort().get(),
            0,
            TEST_KAFKA_TOPIC_2,
            TEST_KAFKA_PARTITION_2,
            KALDB_TEST_CLIENT,
            TEST_S3_BUCKET,
            8081,
            "",
            "");

    // Set it to the new config so that the new kafka writer picks up this config
    SearchContext searchContext2 = new SearchContext("localhost", 10001);
    KaldbConfig.initFromConfigObject(kaldbConfig2);
    ChunkManagerUtil<LogMessage> chunkManagerUtil2 =
        new ChunkManagerUtil<>(
            S3_MOCK_RULE, indexerMetricsRegistry2, 10 * 1024 * 1024 * 1024L, 100, searchContext2);
    indexingServer2 =
        newIndexingServer(chunkManagerUtil2, kaldbConfig2, indexerMetricsRegistry2, 3000);
    indexingServer2.start().get(10, TimeUnit.SECONDS);

    // Produce messages to kafka, so the indexer can consume them.
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    produceMessagesToKafka(broker, startTime, TEST_KAFKA_TOPIC_1, TEST_KAFKA_PARTITION_1);
    await()
        .until(
            () -> {
              try {
                return getCount(MESSAGES_RECEIVED_COUNTER, indexerMetricsRegistry1) == 100;
              } catch (MeterNotFoundException e) {
                return false;
              }
            });

    // Produce messages to kafka, so the indexer can consume them.
    final Instant startTime2 =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    produceMessagesToKafka(broker, startTime2, TEST_KAFKA_TOPIC_2, TEST_KAFKA_PARTITION_2);
    await()
        .until(
            () -> {
              try {
                return getCount(MESSAGES_RECEIVED_COUNTER, indexerMetricsRegistry2) == 100;
              } catch (MeterNotFoundException e) {
                return false;
              }
            });

    queryServer = newQueryServer();
    queryServer.start().get(10, TimeUnit.SECONDS);

    // We want to query the indexing server
    List<String> servers = new ArrayList<>();
    servers.add(String.format("gproto+http://127.0.0.1:%s/", indexingServer1.activeLocalPort()));
    servers.add(String.format("gproto+http://127.0.0.1:%s/", indexingServer2.activeLocalPort()));
    KaldbDistributedQueryService.servers = servers;

    queryServiceStub =
        Clients.newClient(
            String.format("gproto+http://127.0.0.1:%s/", queryServer.activeLocalPort()),
            KaldbServiceGrpc.KaldbServiceBlockingStub.class);
  }

  private static Server newIndexingServer(
      ChunkManagerUtil<LogMessage> chunkManagerUtil,
      KaldbConfigs.KaldbConfig kaldbConfig,
      SimpleMeterRegistry meterRegistry,
      int waitForSearchMs)
      throws InterruptedException, TimeoutException {

    LogMessageTransformer messageTransformer = KaldbIndexer.dataTransformerMap.get("api_log");
    LogMessageWriterImpl logMessageWriterImpl =
        new LogMessageWriterImpl(chunkManagerUtil.chunkManager, messageTransformer);
    KaldbKafkaWriter kafkaWriter = KaldbKafkaWriter.fromConfig(logMessageWriterImpl, meterRegistry);
    kafkaWriter.startAsync();
    kafkaWriter.awaitRunning(DEFAULT_START_STOP_DURATION);

    KaldbIndexer indexer = new KaldbIndexer(chunkManagerUtil.chunkManager, kafkaWriter);
    indexer.startAsync();
    indexer.awaitRunning(DEFAULT_START_STOP_DURATION);

    KaldbLocalQueryService<LogMessage> service =
        new KaldbLocalQueryService<>(indexer.getChunkManager());
    if (waitForSearchMs > 0) {
      KaldbTimeoutLocalQueryService wrapperService =
          new KaldbTimeoutLocalQueryService(service, waitForSearchMs);
      return Server.builder()
          .workerGroup(
              EventLoopGroups.newEventLoopGroup(4, "armeria-common-worker-indexer-delayed", true),
              true)
          .http(kaldbConfig.getIndexerConfig().getServerConfig().getServerPort())
          .verboseResponses(true)
          .service(GrpcService.builder().addService(wrapperService).build())
          .build();
    } else {
      return Server.builder()
          .workerGroup(
              EventLoopGroups.newEventLoopGroup(4, "armeria-common-worker-indexer", true), true)
          .http(kaldbConfig.getIndexerConfig().getServerConfig().getServerPort())
          .verboseResponses(true)
          .service(GrpcService.builder().addService(service).build())
          .build();
    }
  }

  public static Server newQueryServer() {
    KaldbDistributedQueryService service = new KaldbDistributedQueryService();
    return Server.builder()
        .workerGroup(
            EventLoopGroups.newEventLoopGroup(4, "armeria-common-worker-query", true), true)
        // Hardcoding this could mean port collisions b/w tests running in parallel.
        .http(0)
        .verboseResponses(true)
        .service(GrpcService.builder().addService(service).build())
        .build();
  }

  @AfterClass
  public static void shutdownServer() throws Exception {
    if (queryServer != null) {
      queryServer.stop().get(30, TimeUnit.SECONDS);
    }
    if (indexerMetricsRegistry1 != null) {
      indexerMetricsRegistry1.close();
    }
    if (indexingServer1 != null) {
      indexingServer1.stop().get(30, TimeUnit.SECONDS);
    }
    if (indexerMetricsRegistry2 != null) {
      indexerMetricsRegistry2.close();
    }
    if (indexingServer2 != null) {
      indexingServer2.stop().get(30, TimeUnit.SECONDS);
    }
    if (kafkaServer != null) {
      kafkaServer.close();
    }
  }

  // TODO: Add a test for a non-existent server.

  @Test
  public void testSearch() {
    KaldbSearch.SearchResult searchResponse =
        queryServiceStub.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("*:*")
                .setStartTimeEpochMs(0L)
                .setEndTimeEpochMs(1601547099000L)
                .setHowMany(100)
                .setBucketCount(2)
                .build());

    assertThat(searchResponse.getTotalNodes()).isEqualTo(2);
    assertThat(searchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(searchResponse.getTotalCount()).isEqualTo(200);
    assertThat(searchResponse.getHitsCount()).isEqualTo(100);
  }

  @Test
  public void testSearchWithOneShardTimeout() {
    KaldbDistributedQueryService.READ_TIMEOUT_MS = 2000;
    KaldbSearch.SearchResult searchResponse =
        queryServiceStub.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("*:*")
                .setStartTimeEpochMs(0L)
                .setEndTimeEpochMs(1601547099000L)
                .setHowMany(100)
                .setBucketCount(2)
                .build());

    assertThat(searchResponse.getTotalNodes()).isEqualTo(2);
    assertThat(searchResponse.getFailedNodes()).isEqualTo(1);
    assertThat(searchResponse.getTotalCount()).isEqualTo(100);
    assertThat(searchResponse.getHitsCount()).isEqualTo(100);
  }
}
