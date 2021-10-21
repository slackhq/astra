package com.slack.kaldb.server;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.writer.LogMessageTransformer;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.kaldb.writer.kafka.KaldbKafkaWriter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class KaldbIndexerTest {

  private static final String TEST_KAFKA_TOPIC = "test-topic";

  // Kafka producer creates only a partition 0 on first message. So, set the partition to 0 always.
  private static final int TEST_KAFKA_PARTITION = 0;

  private static final String KALDB_TEST_CLIENT = "kaldb-test-client";
  private static final String TEST_S3_BUCKET = "test-s3-bucket";

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private KaldbIndexer kaldbIndexer;
  private SimpleMeterRegistry metricsRegistry;
  private Server server;
  private TestKafkaServer kafkaServer;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        new ChunkManagerUtil<>(S3_MOCK_RULE, metricsRegistry, 10 * 1024 * 1024 * 1024L, 100);

    kafkaServer = new TestKafkaServer();
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.stop().get(30, TimeUnit.SECONDS);
    }
    if (kaldbIndexer != null) {
      kaldbIndexer.stopAsync();
      kaldbIndexer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    kafkaServer.close();
  }

  // TODO: Add a test to ensure Indexer can be shut down cleanly.

  private String uri() {
    return "gproto+http://127.0.0.1:" + server.activeLocalPort() + '/';
  }

  private KaldbSearch.SearchResult searchUsingGrpcApi(
      String queryString, long startTimeMs, long endTimeMs) {
    KaldbServiceGrpc.KaldbServiceBlockingStub kaldbService =
        Clients.newClient(uri(), KaldbServiceGrpc.KaldbServiceBlockingStub.class);

    return kaldbService.search(
        KaldbSearch.SearchRequest.newBuilder()
            .setIndexName(MessageUtil.TEST_INDEX_NAME)
            .setQueryString(queryString)
            .setStartTimeEpochMs(startTimeMs)
            .setEndTimeEpochMs(endTimeMs)
            .setHowMany(10)
            .setBucketCount(2)
            .build());
  }

  @Test
  public void testIndexFromKafkaSearchViaGrpcSearchApi() throws Exception {
    EphemeralKafkaBroker broker = kafkaServer.getBroker();
    assertThat(broker.isRunning()).isTrue();
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

    // Initialize kaldb config.
    KaldbConfigs.KaldbConfig kaldbCfg =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + broker.getKafkaPort().get(),
            0,
            TEST_KAFKA_TOPIC,
            TEST_KAFKA_PARTITION,
            KALDB_TEST_CLIENT,
            TEST_S3_BUCKET,
            8081,
            "",
            "");
    KaldbConfig.initFromConfigObject(kaldbCfg);

    LogMessageTransformer messageTransformer = KaldbIndexer.dataTransformerMap.get("api_log");
    LogMessageWriterImpl logMessageWriterImpl =
        new LogMessageWriterImpl(chunkManager, messageTransformer);
    KaldbKafkaWriter kafkaWriter =
        KaldbKafkaWriter.fromConfig(logMessageWriterImpl, metricsRegistry);
    kafkaWriter.startAsync();
    kafkaWriter.awaitRunning(15, TimeUnit.SECONDS);

    // Create an indexer, an armeria server and register the grpc service.
    ServerBuilder sb = Server.builder();
    sb.http(kaldbCfg.getIndexerConfig().getServerConfig().getServerPort());
    sb.service("/ping", (ctx, req) -> HttpResponse.of("pong!"));
    kaldbIndexer = new KaldbIndexer(chunkManager, kafkaWriter);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    GrpcServiceBuilder searchBuilder =
        GrpcService.builder()
            .addService(new KaldbLocalQueryService<>(kaldbIndexer.getChunkManager()))
            .enableUnframedRequests(true);
    server = sb.service(searchBuilder.build()).build();

    // wait at most 10 seconds to start before throwing an exception
    server.start().get(10, TimeUnit.SECONDS);

    // Produce messages to kafka, so the indexer can consume them.
    produceMessagesToKafka(broker, startTime);

    // No need to commit the active chunk since the last chunk is already closed.
    await().until(() -> chunkManager.getChunkList().size() == 1);
    await().until(() -> getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry) == 100);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry) == 1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(getCount(KaldbKafkaWriter.RECORDS_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(100);
    assertThat(getCount(KaldbKafkaWriter.RECORDS_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);

    // Search for the messages via the grpc API
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    KaldbSearch.SearchResult searchResponse =
        searchUsingGrpcApi("Message1", chunk1StartTimeMs, chunk1StartTimeMs + (100 * 1000));

    // Validate search response
    assertThat(searchResponse.getHitsCount()).isEqualTo(1);
    assertThat(searchResponse.getTookMicros()).isNotZero();
    assertThat(searchResponse.getTotalCount()).isEqualTo(1);
    assertThat(searchResponse.getFailedNodes()).isZero();
    assertThat(searchResponse.getTotalNodes()).isEqualTo(1);
    assertThat(searchResponse.getTotalSnapshots()).isEqualTo(1);
    assertThat(searchResponse.getSnapshotsWithReplicas()).isEqualTo(1);

    chunkManager.stopAsync();
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);

    // TODO: delete expired data cleanly.
  }
}
