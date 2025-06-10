package com.slack.astra.server;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.AstraConfigUtil.makeIndexerConfig;
import static com.slack.astra.testlib.AstraConfigUtil.makeKafkaConfig;
import static com.slack.astra.testlib.ChunkManagerUtil.TEST_HOST;
import static com.slack.astra.testlib.ChunkManagerUtil.TEST_PORT;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.TestKafkaServer.produceMessagesToKafka;
import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.google.common.util.concurrent.Service;
import com.slack.astra.chunk.ReadWriteChunk;
import com.slack.astra.chunk.SearchContext;
import com.slack.astra.chunkManager.RollOverChunkTask;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.recovery.RecoveryTaskMetadata;
import com.slack.astra.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.ChunkManagerUtil;
import com.slack.astra.testlib.TestKafkaServer;
import com.slack.astra.util.QueryBuilderUtil;
import com.slack.astra.writer.kafka.AstraKafkaConsumer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AstraIndexerTest {
  private static final Logger LOG = LoggerFactory.getLogger(AstraIndexerTest.class);

  // TODO: Ensure snapshots are uploaded when indexer shut down happens and shutdown is clean.
  // TODO: Start indexer again and see it works as expected with roll over.

  private static final String TEST_KAFKA_TOPIC = "test-topic";
  private static final int TEST_KAFKA_PARTITION = 0;
  private static final String ASTRA_TEST_CLIENT = "astra-test-client";
  private static final String S3_TEST_BUCKET = "test-astra-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private static final Instant startTime = Instant.now();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private AstraIndexer astraIndexer;
  private SimpleMeterRegistry metricsRegistry;
  private TestKafkaServer kafkaServer;
  private TestingServer testZKServer;
  private AsyncCuratorFramework curatorFramework;
  private AstraConfigs.MetadataStoreConfig metadataStoreConfig;
  private SnapshotMetadataStore snapshotMetadataStore;
  private RecoveryTaskMetadataStore recoveryTaskStore;
  private SearchMetadataStore searchMetadataStore;

  @BeforeEach
  public void setUp() throws Exception {
    AstraConfigs.IndexerConfig indexerConfig = makeIndexerConfig();
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();

    testZKServer = new TestingServer();
    // Metadata store
    metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testZKServer.getConnectString())
                    .setZkPathPrefix("indexerTest")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    curatorFramework =
        spy(CuratorBuilder.build(metricsRegistry, metadataStoreConfig.getZookeeperConfig()));

    chunkManagerUtil =
        new ChunkManagerUtil<>(
            S3_MOCK_EXTENSION,
            S3_TEST_BUCKET,
            metricsRegistry,
            testZKServer,
            10 * 1024 * 1024 * 1024L,
            100,
            new SearchContext(TEST_HOST, TEST_PORT),
            curatorFramework,
            indexerConfig,
            metadataStoreConfig);

    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    snapshotMetadataStore =
        spy(new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, metricsRegistry));
    recoveryTaskStore =
        spy(
            new RecoveryTaskMetadataStore(
                curatorFramework, metadataStoreConfig, metricsRegistry, false));
    searchMetadataStore =
        spy(new SearchMetadataStore(curatorFramework, metadataStoreConfig, metricsRegistry, false));

    kafkaServer = new TestKafkaServer();
  }

  private AstraConfigs.KafkaConfig getKafkaConfig() {
    return makeKafkaConfig(
        TEST_KAFKA_TOPIC,
        TEST_KAFKA_PARTITION,
        ASTRA_TEST_CLIENT,
        kafkaServer.getBroker().getBrokerList().get());
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
    if (astraIndexer != null) {
      astraIndexer.stopAsync();
      astraIndexer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    if (snapshotMetadataStore != null) {
      snapshotMetadataStore.close();
    }
    if (recoveryTaskStore != null) {
      recoveryTaskStore.close();
    }
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }
    if (testZKServer != null) {
      testZKServer.close();
    }
  }

  @Test
  public void testIndexFreshConsumerKafkaSearchViaGrpcSearchApi() throws Exception {
    // Start kafka, produce messages to it and start a search server.
    startKafkaServer();
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // Empty consumer offset since there is no prior consumer.
    astraIndexer =
        new AstraIndexer(
            chunkManagerUtil.chunkManager,
            curatorFramework,
            metadataStoreConfig,
            makeIndexerConfig(1000),
            getKafkaConfig(),
            metricsRegistry);
    astraIndexer.startAsync();
    astraIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);

    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(100, 1);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(2);
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
  }

  @Test
  public void testDeleteStaleSnapshotAndStartConsumerKafkaSearchViaGrpcSearchApi()
      throws Exception {
    startKafkaServer();
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 50;
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(name + "live1", startTimeMs, endTimeMs, maxOffset, "0", 0);
    snapshotMetadataStore.createSync(livePartition1);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsOnly(livePartition1);

    // Empty consumer offset since there is no prior consumer.
    astraIndexer =
        new AstraIndexer(
            chunkManagerUtil.chunkManager,
            curatorFramework,
            metadataStoreConfig,
            makeIndexerConfig(1000),
            getKafkaConfig(),
            metricsRegistry);
    astraIndexer.startAsync();
    astraIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(100, 1);

    // Live snapshot is deleted.
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(2);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .doesNotContain(livePartition1);
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);
  }

  @Test
  public void testExceptionOnIndexerStartup() throws Exception {
    startKafkaServer();
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 50;
    SnapshotMetadata livePartition0 =
        new SnapshotMetadata(name + "live0", startTimeMs, endTimeMs, maxOffset, "0", 0);
    snapshotMetadataStore.createSync(livePartition0);

    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(name + "live1", startTimeMs, endTimeMs, maxOffset, "1", 0);
    snapshotMetadataStore.createSync(livePartition1);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsOnly(livePartition1, livePartition0);

    // Throw exception on initialization
    doThrow(new RuntimeException()).when(curatorFramework).with(any(), any(), any(), any());

    // Empty consumer offset since there is no prior consumer.
    astraIndexer =
        new AstraIndexer(
            chunkManagerUtil.chunkManager,
            curatorFramework,
            metadataStoreConfig,
            makeIndexerConfig(1000),
            getKafkaConfig(),
            metricsRegistry);
    astraIndexer.startAsync();
    await().until(() -> astraIndexer.state() == Service.State.FAILED);
    assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> astraIndexer.startUp());
    astraIndexer = null;
  }

  @Test
  public void testWithMultipleLiveSnapshotsOnIndexerStart() throws Exception {
    startKafkaServer();
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 50;
    SnapshotMetadata livePartition0 =
        new SnapshotMetadata(name + "live0", startTimeMs, endTimeMs, maxOffset, "0", 0);
    snapshotMetadataStore.createSync(livePartition0);

    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(name + "live1", startTimeMs, endTimeMs, maxOffset, "1", 0);
    snapshotMetadataStore.createSync(livePartition1);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsOnly(livePartition1, livePartition0);

    // Empty consumer offset since there is no prior consumer.
    astraIndexer =
        new AstraIndexer(
            chunkManagerUtil.chunkManager,
            curatorFramework,
            metadataStoreConfig,
            makeIndexerConfig(1000),
            getKafkaConfig(),
            metricsRegistry);
    astraIndexer.startAsync();
    astraIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(100, 1);

    // Live snapshot is deleted.
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(livePartition1);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(3);
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);
  }

  @Test
  public void testIndexerStartsWithPreviousOffset() throws Exception {
    startKafkaServer();
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 50;
    SnapshotMetadata livePartition0 =
        new SnapshotMetadata(name + "live0", startTimeMs, endTimeMs, maxOffset, "0", 0);
    snapshotMetadataStore.createSync(livePartition0);

    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(name + "live1", startTimeMs, endTimeMs, maxOffset, "1", 0);
    snapshotMetadataStore.createSync(livePartition1);

    final SnapshotMetadata partition0 =
        new SnapshotMetadata(name, startTimeMs, endTimeMs, maxOffset, "0", 100);
    snapshotMetadataStore.createSync(partition0);

    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsOnly(livePartition1, livePartition0, partition0);

    // Empty consumer offset since there is no prior consumer.
    astraIndexer =
        new AstraIndexer(
            chunkManagerUtil.chunkManager,
            curatorFramework,
            metadataStoreConfig,
            makeIndexerConfig(1000),
            getKafkaConfig(),
            metricsRegistry);
    astraIndexer.startAsync();
    astraIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(49, 0);

    // Live snapshot is deleted.
    List<SnapshotMetadata> snapshots =
        AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots).contains(livePartition1, partition0);
    assertThat(snapshots).doesNotContain(livePartition0);
    assertThat(snapshots.size()).isEqualTo(3);
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);
  }

  @Test
  public void testIndexerCreatesRecoveryTask() throws Exception {
    startKafkaServer();
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 30;
    SnapshotMetadata livePartition0 =
        new SnapshotMetadata(name + "live0", startTimeMs, endTimeMs, maxOffset, "0", 0);
    snapshotMetadataStore.createSync(livePartition0);

    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(name + "live1", startTimeMs, endTimeMs, maxOffset, "1", 0);
    snapshotMetadataStore.createSync(livePartition1);

    final SnapshotMetadata partition0 =
        new SnapshotMetadata(name, startTimeMs, endTimeMs, maxOffset, "0", 100);
    snapshotMetadataStore.createSync(partition0);

    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsOnly(livePartition1, livePartition0, partition0);

    // Empty consumer offset since there is no prior consumer.
    astraIndexer =
        new AstraIndexer(
            chunkManagerUtil.chunkManager,
            curatorFramework,
            metadataStoreConfig,
            makeIndexerConfig(50),
            getKafkaConfig(),
            metricsRegistry);
    astraIndexer.startAsync();
    astraIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    // Produce more messages since the recovery task is created for head.
    produceMessagesToKafka(kafkaServer.getBroker(), startTime);

    consumeMessagesAndSearchMessagesTest(100, 1);

    // Live snapshot is deleted, recovery task is created.
    List<SnapshotMetadata> snapshots =
        AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots).contains(livePartition1, partition0);
    assertThat(snapshots).doesNotContain(livePartition0);
    assertThat(snapshots.size()).isEqualTo(4);
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore).size()).isEqualTo(1);
    RecoveryTaskMetadata recoveryTask1 =
        AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore).get(0);
    assertThat(recoveryTask1.startOffset).isEqualTo(31);
    assertThat(recoveryTask1.endOffset).isEqualTo(99);
    assertThat(recoveryTask1.partitionId).isEqualTo("0");
  }

  @Test
  public void testIndexerShutdownTwice() throws Exception {
    startKafkaServer();
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 30;
    SnapshotMetadata livePartition0 =
        new SnapshotMetadata(name + "live0", startTimeMs, endTimeMs, maxOffset, "0", 0);
    snapshotMetadataStore.createSync(livePartition0);

    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(name + "live1", startTimeMs, endTimeMs, maxOffset, "1", 0);
    snapshotMetadataStore.createSync(livePartition1);

    final SnapshotMetadata partition0 =
        new SnapshotMetadata(name, startTimeMs, endTimeMs, maxOffset, "0", 100);
    snapshotMetadataStore.createSync(partition0);

    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsOnly(livePartition1, livePartition0, partition0);

    // Empty consumer offset since there is no prior consumer.
    astraIndexer =
        new AstraIndexer(
            chunkManagerUtil.chunkManager,
            curatorFramework,
            metadataStoreConfig,
            makeIndexerConfig(50),
            getKafkaConfig(),
            metricsRegistry);
    astraIndexer.startAsync();
    astraIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    // Produce more messages since the recovery task is created for head.
    produceMessagesToKafka(kafkaServer.getBroker(), startTime);

    consumeMessagesAndSearchMessagesTest(100, 1);

    // Live snapshot is deleted, recovery task is created.
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(livePartition1, partition0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(4);
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore).size()).isEqualTo(1);
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);
    RecoveryTaskMetadata recoveryTask1 =
        AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore).get(0);
    assertThat(recoveryTask1.startOffset).isEqualTo(31);
    assertThat(recoveryTask1.endOffset).isEqualTo(99);
    assertThat(recoveryTask1.partitionId).isEqualTo("0");

    // Shutting down is idempotent. So, doing it twice shouldn't throw an error.
    astraIndexer.shutDown();
    astraIndexer.shutDown();
    astraIndexer = null;
  }

  @Test
  public void testIndexerRestart() throws Exception {
    startKafkaServer();
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore)).isEmpty();

    // Create a live partition for this partiton
    final String name = "testSnapshotId";
    final long startTimeMs = 1;
    final long endTimeMs = 100;
    final long maxOffset = 30;
    SnapshotMetadata livePartition0 =
        new SnapshotMetadata(name + "live0", startTimeMs, endTimeMs, maxOffset, "0", 0);
    snapshotMetadataStore.createSync(livePartition0);

    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(name + "live1", startTimeMs, endTimeMs, maxOffset, "1", 0);
    snapshotMetadataStore.createSync(livePartition1);

    final SnapshotMetadata partition0 =
        new SnapshotMetadata(name, startTimeMs, endTimeMs, maxOffset, "0", 100);
    snapshotMetadataStore.createSync(partition0);

    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsOnly(livePartition1, livePartition0, partition0);

    // Empty consumer offset since there is no prior consumer.
    astraIndexer =
        new AstraIndexer(
            chunkManagerUtil.chunkManager,
            curatorFramework,
            metadataStoreConfig,
            makeIndexerConfig(1000),
            getKafkaConfig(),
            metricsRegistry);
    astraIndexer.startAsync();
    astraIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    // Consume messages from offset 31 to 100.
    consumeMessagesAndSearchMessagesTest(69, 0);

    // Live snapshot is deleted, no recovery task is created.
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(livePartition1, partition0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .doesNotContain(livePartition0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(3);
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore).size()).isZero();
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);

    // Shutting down is idempotent. So, doing it twice shouldn't throw an error.
    astraIndexer.stopAsync();
    chunkManagerUtil.chunkManager.stopAsync();
    astraIndexer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManagerUtil.chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);

    // await().until(() -> kafkaServer.getConnectedConsumerGroups() == 0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(livePartition1, partition0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .doesNotContain(livePartition0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(2);
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore).size()).isZero();
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore)).isEmpty();

    // start indexer again. The indexer should index the same data again.
    LOG.info("Starting the indexer again");
    chunkManagerUtil =
        new ChunkManagerUtil<>(
            S3_MOCK_EXTENSION,
            S3_TEST_BUCKET,
            metricsRegistry,
            testZKServer,
            10 * 1024 * 1024 * 1024L,
            100,
            new SearchContext(TEST_HOST, TEST_PORT),
            curatorFramework,
            makeIndexerConfig(),
            metadataStoreConfig);
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    astraIndexer =
        new AstraIndexer(
            chunkManagerUtil.chunkManager,
            curatorFramework,
            metadataStoreConfig,
            makeIndexerConfig(1000),
            getKafkaConfig(),
            metricsRegistry);
    astraIndexer.startAsync();
    astraIndexer.awaitRunning(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    consumeMessagesAndSearchMessagesTest(138, 0);

    // Live snapshot is deleted, recovery task is created.
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(livePartition1, partition0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .doesNotContain(livePartition0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);
  }

  private void startKafkaServer() throws Exception {
    EphemeralKafkaBroker broker = kafkaServer.getBroker();
    assertThat(broker.isRunning()).isTrue();

    // Produce messages to kafka, so the indexer can consume them.
    produceMessagesToKafka(broker, startTime);
  }

  private void consumeMessagesAndSearchMessagesTest(int messagesReceived, double rolloversCompleted)
      throws IOException {
    // commit the active chunk if it exists, else it was rolled over.
    final ReadWriteChunk<LogMessage> activeChunk = chunkManagerUtil.chunkManager.getActiveChunk();
    if (activeChunk != null) {
      activeChunk.commit();
    }

    await().until(() -> getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry) == messagesReceived);
    assertThat(chunkManagerUtil.chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    if (rolloversCompleted > 0) {
      await()
          .until(
              () ->
                  getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)
                      == rolloversCompleted);
      await()
          .until(
              () ->
                  getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)
                      == rolloversCompleted);
      assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    }
    assertThat(getCount(AstraKafkaConsumer.RECORDS_RECEIVED_COUNTER, metricsRegistry))
        .isEqualTo(messagesReceived);
    assertThat(getCount(AstraKafkaConsumer.RECORDS_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);

    // Search for the messages via the grpc API
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    SearchResult<LogMessage> searchResult =
        chunkManagerUtil.chunkManager.query(
            new SearchQuery(
                "test",
                chunk1StartTimeMs,
                chunk1StartTimeMs + (100 * 1000),
                10,
                Collections.emptyList(),
                QueryBuilderUtil.generateQueryBuilder(
                    "Message100", chunk1StartTimeMs, chunk1StartTimeMs + (100 * 1000)),
                null,
                createGenericDateHistogramAggregatorFactoriesBuilder()),
            Duration.ofMillis(3000));

    // Validate search response
    assertThat(searchResult.hits.size()).isEqualTo(1);
    assertThat(searchResult.tookMicros).isNotZero();
    assertThat(searchResult.failedNodes).isZero();
    assertThat(searchResult.totalNodes).isEqualTo(1);
    assertThat(searchResult.totalSnapshots).isEqualTo(1);
    assertThat(searchResult.snapshotsWithReplicas).isEqualTo(1);
  }
}
