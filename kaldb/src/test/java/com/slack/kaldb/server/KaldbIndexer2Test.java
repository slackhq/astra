package com.slack.kaldb.server;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.metadata.snapshot.SnapshotMetadata.LIVE_SNAPSHOT_PATH;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.slack.kaldb.chunk.ReadWriteChunk;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.writer.kafka.KaldbKafkaWriter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbIndexer2Test {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbIndexer2Test.class);

  // TODO: Test recovery task creation and indexing from a head offset.
  // TODO: Test exception in pre-startup
  // TODO: Test exception in actual start up method.
  // TODO: Ensure clean shutdown happens on indexer shutdown.
  // TODO: Ensure end to end query API with indexer and query:
  // TODO: Ensure snapshots are uploaded when indexer shut down happens.
  // TODO: Ensure Closing the kafka consumer twice is ok.
  // TODO: Add a test to ensure Indexer can be shut down cleanly.

  private static final String TEST_KAFKA_TOPIC = "test-topic";

  // Kafka producer creates only a partition 0 on first message. So, set the partition to 0 always.
  private static final int TEST_KAFKA_PARTITION = 0;
  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static final String KALDB_TEST_CLIENT = "kaldb-test-client";

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final Instant startTime =
      LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private KaldbIndexer2 kaldbIndexer;
  private SimpleMeterRegistry metricsRegistry;
  private Server armeriaServer;
  private TestKafkaServer kafkaServer;
  private TestingServer testZKServer;
  private MetadataStore zkMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private RecoveryTaskMetadataStore recoveryTaskStore;

  @Before
  public void setUp() throws Exception {
    // TODO: Remove config initialization once we no longer use KaldbConfig directly.
    // Initialize kaldb config.
    KaldbConfigs.KaldbConfig kaldbCfg =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:90901",
            0,
            TEST_KAFKA_TOPIC,
            TEST_KAFKA_PARTITION,
            KALDB_TEST_CLIENT,
            TEST_S3_BUCKET,
            8081,
            "",
            "");
    KaldbConfig.initFromConfigObject(kaldbCfg);

    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        new ChunkManagerUtil<>(S3_MOCK_RULE, metricsRegistry, 10 * 1024 * 1024 * 1024L, 100);
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Metadata store
    testZKServer = new TestingServer();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testZKServer.getConnectString())
            .setZkPathPrefix("RecoveryTaskAssignmentServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    zkMetadataStore = ZookeeperMetadataStoreImpl.fromConfig(metricsRegistry, zkConfig);
    snapshotMetadataStore = spy(new SnapshotMetadataStore(zkMetadataStore, false));
    recoveryTaskStore = spy(new RecoveryTaskMetadataStore(zkMetadataStore, false));

    kafkaServer = new TestKafkaServer();
  }

  private KaldbConfigs.KafkaConfig makeKafkaConfig() {
    return KaldbConfigs.KafkaConfig.newBuilder()
        .setKafkaTopic(TEST_KAFKA_TOPIC)
        .setKafkaTopicPartition(String.valueOf(TEST_KAFKA_PARTITION))
        .setKafkaBootStrapServers(kafkaServer.getBroker().getBrokerList().get())
        .setKafkaClientGroup(KALDB_TEST_CLIENT)
        .setEnableKafkaAutoCommit("true")
        .setKafkaAutoCommitInterval("5000")
        .setKafkaSessionTimeout("30000")
        .build();
  }

  private KaldbConfigs.IndexerConfig makeIndexerConfig(int maxOffsetDelay) {
    return KaldbConfigs.IndexerConfig.newBuilder()
        .setMaxOffsetDelay(maxOffsetDelay)
        .setDataTransformer("api_log")
        .build();
  }

  @After
  public void tearDown() throws Exception {
    if (armeriaServer != null) {
      armeriaServer.stop().get(30, TimeUnit.SECONDS);
    }
    chunkManagerUtil.close();
    if (kaldbIndexer != null) {
      kaldbIndexer.stopAsync();
      kaldbIndexer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    kafkaServer.close();
    snapshotMetadataStore.close();
    recoveryTaskStore.close();
    zkMetadataStore.close();
    testZKServer.close();
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

  private String uri() {
    return "gproto+http://127.0.0.1:" + armeriaServer.activeLocalPort() + '/';
  }

  @Test
  public void testIndexFreshConsumerKafkaSearchViaGrpcSearchApi() throws Exception {
    // Start kafka, produce messages to it and start a search server.
    startKafkaAndSearchServer();
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    // Empty consumer offset since there is no prior consumer.
    kaldbIndexer =
        new KaldbIndexer2(
            chunkManager,
            zkMetadataStore,
            makeIndexerConfig(1000),
            makeKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);

    // TODO: Should be 0?
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(100, 1);
  }

  @Test
  public void testDeleteStaleSnapshotAndStartConsumerKafkaSearchViaGrpcSearchApi()
      throws Exception {
    startKafkaAndSearchServer();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 50;
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "live1", LIVE_SNAPSHOT_PATH, startTimeMs, endTimeMs, maxOffset, "0");
    snapshotMetadataStore.createSync(livePartition1);
    assertThat(snapshotMetadataStore.listSync()).containsOnly(livePartition1);

    // Empty consumer offset since there is no prior consumer.
    kaldbIndexer =
        new KaldbIndexer2(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(1000),
            makeKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    // TODO: Should be 0?
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(100, 1);

    // Live snapshot is deleted.
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
  }

  @Test
  public void testWithMultipleLiveSnapshotsOnIndexerStart() throws Exception {
    startKafkaAndSearchServer();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 50;
    SnapshotMetadata livePartition0 =
        new SnapshotMetadata(
            name + "live0", LIVE_SNAPSHOT_PATH, startTimeMs, endTimeMs, maxOffset, "0");
    snapshotMetadataStore.createSync(livePartition0);

    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "live1", LIVE_SNAPSHOT_PATH, startTimeMs, endTimeMs, maxOffset, "1");
    snapshotMetadataStore.createSync(livePartition1);
    assertThat(snapshotMetadataStore.listSync()).containsOnly(livePartition1, livePartition0);

    // Empty consumer offset since there is no prior consumer.
    kaldbIndexer =
        new KaldbIndexer2(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(1000),
            makeKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    // TODO: Should be 0?
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(100, 1);

    // Live snapshot is deleted.
    assertThat(snapshotMetadataStore.listSync()).containsOnly(livePartition1);
  }

  @Test
  public void testIndexerStartsWithPreviousOffset() throws Exception {
    startKafkaAndSearchServer();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 50;
    SnapshotMetadata livePartition0 =
        new SnapshotMetadata(
            name + "live0", LIVE_SNAPSHOT_PATH, startTimeMs, endTimeMs, maxOffset, "0");
    snapshotMetadataStore.createSync(livePartition0);

    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "live1", LIVE_SNAPSHOT_PATH, startTimeMs, endTimeMs, maxOffset, "1");
    snapshotMetadataStore.createSync(livePartition1);

    final SnapshotMetadata partition0 =
        new SnapshotMetadata(name, path, startTimeMs, endTimeMs, maxOffset, "0");
    snapshotMetadataStore.createSync(partition0);

    assertThat(snapshotMetadataStore.listSync())
        .containsOnly(livePartition1, livePartition0, partition0);

    // Empty consumer offset since there is no prior consumer.
    kaldbIndexer =
        new KaldbIndexer2(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(1000),
            makeKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    // TODO: Should be 0?
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(49, 0);

    // Live snapshot is deleted.
    assertThat(snapshotMetadataStore.listSync()).containsOnly(livePartition1, partition0);
  }

  private void startKafkaAndSearchServer() throws Exception {
    EphemeralKafkaBroker broker = kafkaServer.getBroker();
    assertThat(broker.isRunning()).isTrue();
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    // Create an indexer, an armeria server and register the grpc service.
    ServerBuilder sb = Server.builder();
    // sb.http(kaldbCfg.getIndexerConfig().getServerConfig().getServerPort());
    sb.http(8081);
    sb.service("/ping", (ctx, req) -> HttpResponse.of("pong!"));

    // Produce messages to kafka, so the indexer can consume them.
    produceMessagesToKafka(broker, startTime);

    GrpcServiceBuilder searchBuilder =
        GrpcService.builder()
            .addService(new KaldbLocalQueryService<>(chunkManager))
            .enableUnframedRequests(true);
    armeriaServer = sb.service(searchBuilder.build()).build();
    // wait at most 10 seconds to start before throwing an exception
    armeriaServer.start().get(10, TimeUnit.SECONDS);
  }

  private void consumeMessagesAndSearchMessagesTest(
      int messagesReceived, double rolloversCompleted) {
    // commit the active chunk if it exists, else it was rolled over.
    final ReadWriteChunk<LogMessage> activeChunk = chunkManagerUtil.chunkManager.getActiveChunk();
    if (activeChunk != null) {
      activeChunk.commit();
    }

    await().until(() -> getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry) == messagesReceived);
    assertThat(chunkManagerUtil.chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    if (rolloversCompleted > 0) {
      assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry))
          .isEqualTo(rolloversCompleted);
      await()
          .until(
              () ->
                  getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)
                      == rolloversCompleted);
      assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    }
    assertThat(getCount(KaldbKafkaWriter.RECORDS_RECEIVED_COUNTER, metricsRegistry))
        .isEqualTo(messagesReceived);
    assertThat(getCount(KaldbKafkaWriter.RECORDS_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);

    // Search for the messages via the grpc API
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    KaldbSearch.SearchResult searchResponse =
        searchUsingGrpcApi("Message100", chunk1StartTimeMs, chunk1StartTimeMs + (100 * 1000));

    // Validate search response
    assertThat(searchResponse.getHitsCount()).isEqualTo(1);
    assertThat(searchResponse.getTookMicros()).isNotZero();
    assertThat(searchResponse.getTotalCount()).isEqualTo(1);
    assertThat(searchResponse.getFailedNodes()).isZero();
    assertThat(searchResponse.getTotalNodes()).isEqualTo(1);
    assertThat(searchResponse.getTotalSnapshots()).isEqualTo(1);
    assertThat(searchResponse.getSnapshotsWithReplicas()).isEqualTo(1);
  }
}
