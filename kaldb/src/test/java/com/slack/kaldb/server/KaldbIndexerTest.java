package com.slack.kaldb.server;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.metadata.snapshot.SnapshotMetadata.LIVE_SNAPSHOT_PATH;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.*;
import static com.slack.kaldb.testlib.KaldbConfigUtil.*;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.google.common.util.concurrent.Service;
import com.slack.kaldb.chunk.ReadWriteChunk;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.writer.kafka.KaldbKafkaConsumer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbIndexerTest {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbIndexerTest.class);

  // TODO: Ensure snapshots are uploaded when indexer shut down happens and shutdown is clean.
  // TODO: Start indexer again and see it works as expected with roll over.

  private static final String TEST_KAFKA_TOPIC = "test-topic";
  private static final int TEST_KAFKA_PARTITION = 0;
  private static final String KALDB_TEST_CLIENT = "kaldb-test-client";

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final Instant startTime =
      LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private KaldbIndexer kaldbIndexer;
  private SimpleMeterRegistry metricsRegistry;
  private TestKafkaServer kafkaServer;
  private TestingServer testZKServer;
  private MetadataStore zkMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private RecoveryTaskMetadataStore recoveryTaskStore;
  private SearchMetadataStore searchMetadataStore;

  @Before
  public void setUp() throws Exception {
    KaldbConfigs.IndexerConfig indexerConfig = makeIndexerConfig();
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();

    testZKServer = new TestingServer();
    // Metadata store
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testZKServer.getConnectString())
            .setZkPathPrefix("indexerTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    zkMetadataStore = spy(ZookeeperMetadataStoreImpl.fromConfig(metricsRegistry, zkConfig));

    chunkManagerUtil =
        new ChunkManagerUtil<>(
            S3_MOCK_RULE,
            metricsRegistry,
            testZKServer,
            10 * 1024 * 1024 * 1024L,
            100,
            new SearchContext(TEST_HOST, TEST_PORT),
            zkMetadataStore,
            indexerConfig);

    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    snapshotMetadataStore = spy(new SnapshotMetadataStore(zkMetadataStore, false));
    recoveryTaskStore = spy(new RecoveryTaskMetadataStore(zkMetadataStore, false));
    searchMetadataStore = spy(new SearchMetadataStore(zkMetadataStore, false));

    kafkaServer = new TestKafkaServer();
  }

  private KaldbConfigs.KafkaConfig getKafkaConfig() {
    return makeKafkaConfig(
        TEST_KAFKA_TOPIC,
        TEST_KAFKA_PARTITION,
        KALDB_TEST_CLIENT,
        kafkaServer.getBroker().getBrokerList().get());
  }

  @After
  public void tearDown() throws Exception {
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

  @Test
  public void testIndexFreshConsumerKafkaSearchViaGrpcSearchApi() throws Exception {
    // Start kafka, produce messages to it and start a search server.
    startKafkaServer();
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // Empty consumer offset since there is no prior consumer.
    kaldbIndexer =
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(1000, "api_log"),
            getKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);

    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(100, 1);
    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);
    assertThat(recoveryTaskStore.listSync()).isEmpty();
  }

  @Test
  public void testDeleteStaleSnapshotAndStartConsumerKafkaSearchViaGrpcSearchApi()
      throws Exception {
    startKafkaServer();
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
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
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(1000, "api_log"),
            getKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(100, 1);

    // Live snapshot is deleted.
    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(snapshotMetadataStore.listSync()).doesNotContain(livePartition1);
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);
  }

  @Test
  public void testExceptionOnIndexerStartup() throws Exception {
    startKafkaServer();
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
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

    // Throw exception on a list call.
    doThrow(new RuntimeException()).when(zkMetadataStore).get(any());

    // Empty consumer offset since there is no prior consumer.
    kaldbIndexer =
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(1000, "api_log"),
            getKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    await().until(() -> kaldbIndexer.state() == Service.State.FAILED);
    assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> kaldbIndexer.startUp());
    kaldbIndexer = null;
  }

  @Test
  public void testWithMultipleLiveSnapshotsOnIndexerStart() throws Exception {
    startKafkaServer();
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
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
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(1000, "api_log"),
            getKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(100, 1);

    // Live snapshot is deleted.
    assertThat(snapshotMetadataStore.listSync()).contains(livePartition1);
    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);
  }

  @Test
  public void testIndexerStartsWithPreviousOffset() throws Exception {
    startKafkaServer();
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

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
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(1000, "api_log"),
            getKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(49, 0);

    // Live snapshot is deleted.
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    assertThat(snapshots).contains(livePartition1, partition0);
    assertThat(snapshots).doesNotContain(livePartition0);
    assertThat(snapshots.size()).isEqualTo(3);
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);
  }

  @Test
  public void testIndexerCreatesRecoveryTask() throws Exception {
    startKafkaServer();
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 30;
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
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(50, "api_log"),
            getKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    // Produce more messages since the recovery task is created for head.
    produceMessagesToKafka(kafkaServer.getBroker(), startTime);

    consumeMessagesAndSearchMessagesTest(100, 1);

    // Live snapshot is deleted, recovery task is created.
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    assertThat(snapshots).contains(livePartition1, partition0);
    assertThat(snapshots).doesNotContain(livePartition0);
    assertThat(snapshots.size()).isEqualTo(4);
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);
    assertThat(recoveryTaskStore.listSync().size()).isEqualTo(1);
    RecoveryTaskMetadata recoveryTask1 = recoveryTaskStore.listSync().get(0);
    assertThat(recoveryTask1.startOffset).isEqualTo(31);
    assertThat(recoveryTask1.endOffset).isEqualTo(99);
    assertThat(recoveryTask1.partitionId).isEqualTo("0");
  }

  @Test
  public void testIndexerShutdownTwice() throws Exception {
    startKafkaServer();
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 30;
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
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(50, "api_log"),
            getKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    // Produce more messages since the recovery task is created for head.
    produceMessagesToKafka(kafkaServer.getBroker(), startTime);

    consumeMessagesAndSearchMessagesTest(100, 1);

    // Live snapshot is deleted, recovery task is created.
    assertThat(snapshotMetadataStore.listSync()).contains(livePartition1, partition0);
    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(4);
    assertThat(recoveryTaskStore.listSync().size()).isEqualTo(1);
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);
    RecoveryTaskMetadata recoveryTask1 = recoveryTaskStore.listSync().get(0);
    assertThat(recoveryTask1.startOffset).isEqualTo(31);
    assertThat(recoveryTask1.endOffset).isEqualTo(99);
    assertThat(recoveryTask1.partitionId).isEqualTo("0");

    // Shutting down is idempotent. So, doing it twice shouldn't throw an error.
    kaldbIndexer.shutDown();
    kaldbIndexer.shutDown();
    kaldbIndexer = null;
  }

  @Test
  public void testIndexerRestart() throws Exception {
    startKafkaServer();
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    assertThat(searchMetadataStore.listSync()).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 30;
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
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(1000, "api_log"),
            getKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    // Consume messages from offset 31 to 100.
    consumeMessagesAndSearchMessagesTest(69, 0);

    // Live snapshot is deleted, no recovery task is created.
    assertThat(snapshotMetadataStore.listSync()).contains(livePartition1, partition0);
    assertThat(snapshotMetadataStore.listSync()).doesNotContain(livePartition0);
    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(recoveryTaskStore.listSync().size()).isZero();
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);

    // Shutting down is idempotent. So, doing it twice shouldn't throw an error.
    kaldbIndexer.stopAsync();
    chunkManagerUtil.chunkManager.stopAsync();
    kaldbIndexer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManagerUtil.chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);

    // await().until(() -> kafkaServer.getConnectedConsumerGroups() == 0);
    assertThat(snapshotMetadataStore.listSync()).contains(livePartition1, partition0);
    assertThat(snapshotMetadataStore.listSync()).doesNotContain(livePartition0);
    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(recoveryTaskStore.listSync().size()).isZero();
    assertThat(searchMetadataStore.listSync()).isEmpty();

    // start indexer again. The indexer should index the same data again.
    LOG.info("Starting the indexer again");
    chunkManagerUtil =
        new ChunkManagerUtil<>(
            S3_MOCK_RULE,
            metricsRegistry,
            testZKServer,
            10 * 1024 * 1024 * 1024L,
            100,
            new SearchContext(TEST_HOST, TEST_PORT),
            zkMetadataStore,
            makeIndexerConfig());
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    kaldbIndexer =
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            zkMetadataStore,
            makeIndexerConfig(1000, "api_log"),
            getKafkaConfig(),
            metricsRegistry);
    kaldbIndexer.startAsync();
    kaldbIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(138, 0);

    // Live snapshot is deleted, recovery task is created.
    assertThat(snapshotMetadataStore.listSync()).contains(livePartition1, partition0);
    assertThat(snapshotMetadataStore.listSync()).doesNotContain(livePartition0);
    assertThat(recoveryTaskStore.listSync()).isEmpty();
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);
  }

  private void startKafkaServer() throws Exception {
    EphemeralKafkaBroker broker = kafkaServer.getBroker();
    assertThat(broker.isRunning()).isTrue();

    // Produce messages to kafka, so the indexer can consume them.
    produceMessagesToKafka(broker, startTime);
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
    assertThat(getCount(KaldbKafkaConsumer.RECORDS_RECEIVED_COUNTER, metricsRegistry))
        .isEqualTo(messagesReceived);
    assertThat(getCount(KaldbKafkaConsumer.RECORDS_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);

    // Search for the messages via the grpc API
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    SearchResult<LogMessage> searchResult =
        chunkManagerUtil.chunkManager.query(
            new SearchQuery(
                "test", "Message100", chunk1StartTimeMs, chunk1StartTimeMs + (100 * 1000), 10, 2));

    // Validate search response
    assertThat(searchResult.hits.size()).isEqualTo(1);
    assertThat(searchResult.tookMicros).isNotZero();
    assertThat(searchResult.totalCount).isEqualTo(1);
    assertThat(searchResult.failedNodes).isZero();
    assertThat(searchResult.totalNodes).isEqualTo(1);
    assertThat(searchResult.totalSnapshots).isEqualTo(1);
    assertThat(searchResult.snapshotsWithReplicas).isEqualTo(1);
  }
}
