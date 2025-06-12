package com.slack.astra.chunkManager;

import static com.slack.astra.chunk.ChunkInfo.MAX_FUTURE_TIME;
import static com.slack.astra.chunkManager.IndexingChunkManager.LIVE_BYTES_INDEXED;
import static com.slack.astra.chunkManager.IndexingChunkManager.LIVE_MESSAGES_INDEXED;
import static com.slack.astra.chunkManager.RollOverChunkTask.ROLLOVERS_COMPLETED;
import static com.slack.astra.chunkManager.RollOverChunkTask.ROLLOVERS_FAILED;
import static com.slack.astra.chunkManager.RollOverChunkTask.ROLLOVERS_INITIATED;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.server.AstraConfig.CHUNK_DATA_PREFIX;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.ChunkManagerUtil.fetchLiveSnapshot;
import static com.slack.astra.testlib.ChunkManagerUtil.fetchNonLiveSnapshot;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.MetricsUtil.getValue;
import static com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.chunk.ChunkInfo;
import com.slack.astra.chunk.ReadWriteChunk;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.testlib.AstraConfigUtil;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension;
import com.slack.astra.util.QueryBuilderUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class RecoveryChunkManagerTest {
  // TODO: Ensure clean close after all chunks are uploaded.
  // TODO: Test post snapshot for recovery.

  private static final String S3_TEST_BUCKET = "test-astra-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private static final String TEST_KAFKA_PARTITION_ID = "10";

  private RecoveryChunkManager<LogMessage> chunkManager = null;

  private SimpleMeterRegistry metricsRegistry;
  private S3AsyncClient s3AsyncClient;

  private static final String ZK_PATH_PREFIX = "testZK";
  private BlobStore blobStore;
  private TestingServer localZkServer;
  private AsyncCuratorFramework curatorFramework;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;

  private AstraConfigs.AstraConfig AstraConfig;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    // create an S3 client.
    s3AsyncClient = S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    blobStore = new BlobStore(s3AsyncClient, S3_TEST_BUCKET);

    localZkServer = new TestingServer();
    localZkServer.start();

    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(localZkServer.getConnectString())
            .setZkPathPrefix(ZK_PATH_PREFIX)
            .setZkSessionTimeoutMs(15000)
            .setZkConnectionTimeoutMs(1500)
            .setSleepBetweenRetriesMs(1000)
            .setZkCacheInitTimeoutMs(1000)
            .build();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(zkConfig)
            .build();

    curatorFramework = CuratorBuilder.build(metricsRegistry, zkConfig);
    searchMetadataStore =
        new SearchMetadataStore(curatorFramework, metadataStoreConfig, metricsRegistry, false);
    snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, metricsRegistry);
  }

  @AfterEach
  public void tearDown() throws TimeoutException, IOException, InterruptedException {
    metricsRegistry.close();
    if (chunkManager != null) {
      chunkManager.stopAsync();
      chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    curatorFramework.unwrap().close();
    s3AsyncClient.close();
    localZkServer.stop();
  }

  private void initChunkManager(BlobStore blobStore) throws Exception {
    AstraConfig =
        AstraConfigUtil.makeAstraConfig(
            "localhost:9090",
            9000,
            "testKafkaTopic",
            0,
            "astra_test_client",
            S3_TEST_BUCKET,
            9000 + 1,
            "localhost:2181",
            "recoveryZK_",
            AstraConfigs.NodeRole.RECOVERY,
            10000,
            9003,
            100);

    chunkManager =
        RecoveryChunkManager.fromConfig(
            metricsRegistry,
            searchMetadataStore,
            snapshotMetadataStore,
            AstraConfig.getIndexerConfig(),
            blobStore);

    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void testAddMessageAndRollover() throws Exception {
    initChunkManager(blobStore);

    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now());
    int actualChunkSize = 0;
    int offset = 1;
    for (Trace.Span m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      actualChunkSize += msgSize;
      offset++;
    }
    ReadWriteChunk<LogMessage> currentChunk = chunkManager.getActiveChunk();
    currentChunk.commit();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(100);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(100);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(actualChunkSize);

    // Check metadata registration. No metadata registration until rollover.
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore)).isEmpty();

    // Search query
    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            0,
            MAX_TIME,
            10,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", 0L, MAX_TIME),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    SearchResult<LogMessage> results = chunkManager.getActiveChunk().query(searchQuery);
    assertThat(results.hits.size()).isEqualTo(1);

    // Test chunk metadata.
    ChunkInfo chunkInfo = chunkManager.getActiveChunk().info();
    assertThat(chunkInfo.getChunkSnapshotTimeEpochMs()).isZero();
    assertThat(chunkInfo.getDataStartTimeEpochMs()).isGreaterThan(0);
    assertThat(chunkInfo.getDataEndTimeEpochMs()).isGreaterThan(0);
    assertThat(chunkInfo.chunkId).startsWith(CHUNK_DATA_PREFIX);
    assertThat(chunkInfo.getMaxOffset()).isEqualTo(offset - 1);
    assertThat(chunkInfo.getDataStartTimeEpochMs())
        .isEqualTo(
            TimeUnit.MILLISECONDS.convert(messages.get(0).getTimestamp(), TimeUnit.MICROSECONDS));
    assertThat(chunkInfo.getDataEndTimeEpochMs())
        .isEqualTo(
            TimeUnit.MILLISECONDS.convert(messages.get(99).getTimestamp(), TimeUnit.MICROSECONDS));

    // Add a message with a very high offset.
    final int veryHighOffset = 1000;
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(offset - 1);
    assertThat(veryHighOffset - offset).isGreaterThan(100);
    Trace.Span messageWithHighOffset = SpanUtil.makeSpan(101);
    chunkManager.addMessage(
        messageWithHighOffset,
        messageWithHighOffset.toString().length(),
        TEST_KAFKA_PARTITION_ID,
        veryHighOffset);
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(veryHighOffset);
    chunkManager.getActiveChunk().commit();
    assertThat(
            chunkManager
                .getActiveChunk()
                .query(
                    new SearchQuery(
                        MessageUtil.TEST_DATASET_NAME,
                        0,
                        MAX_TIME,
                        10,
                        Collections.emptyList(),
                        QueryBuilderUtil.generateQueryBuilder("Message101", 0L, MAX_TIME),
                        null,
                        createGenericDateHistogramAggregatorFactoriesBuilder()))
                .hits
                .size())
        .isEqualTo(1);

    // Add a message with a lower offset.
    final int lowerOffset = 500;
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(veryHighOffset);
    assertThat(lowerOffset - offset).isGreaterThan(100);
    assertThat(veryHighOffset - lowerOffset).isGreaterThan(100);
    Trace.Span messageWithLowerOffset = SpanUtil.makeSpan(102);
    chunkManager.addMessage(
        messageWithLowerOffset,
        messageWithLowerOffset.toString().length(),
        TEST_KAFKA_PARTITION_ID,
        lowerOffset);
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(veryHighOffset);
    chunkManager.getActiveChunk().commit();
    assertThat(
            chunkManager
                .getActiveChunk()
                .query(
                    new SearchQuery(
                        MessageUtil.TEST_DATASET_NAME,
                        0,
                        MAX_TIME,
                        10,
                        Collections.emptyList(),
                        QueryBuilderUtil.generateQueryBuilder("Message102", 0L, MAX_TIME),
                        null,
                        createGenericDateHistogramAggregatorFactoriesBuilder()))
                .hits
                .size())
        .isEqualTo(1);

    // Inserting a message from a different kafka partition fails
    Trace.Span messageWithInvalidTopic = SpanUtil.makeSpan(103);
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    messageWithInvalidTopic,
                    messageWithInvalidTopic.toString().length(),
                    "differentKafkaTopic",
                    lowerOffset + 1));

    // Get the count of the amount of indices so that we can confirm we've cleaned them up
    // after the rollover
    final File dataDirectory = new File(AstraConfig.getIndexerConfig().getDataDirectory());
    final File indexDirectory = new File(dataDirectory.getAbsolutePath() + "/indices");
    File[] filesBeforeRollover = indexDirectory.listFiles();
    assertThat(filesBeforeRollover).isNotNull();
    assertThat(filesBeforeRollover).isNotEmpty();

    // Roll over chunk.
    assertThat(chunkManager.waitForRollOvers()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore)).isEmpty();
    List<SnapshotMetadata> snapshots =
        AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(snapshots.get(0).startTimeEpochMs)
        .isEqualTo(
            TimeUnit.MILLISECONDS.convert(messages.get(0).getTimestamp(), TimeUnit.MICROSECONDS));
    assertThat(snapshots.get(0).endTimeEpochMs)
        .isGreaterThanOrEqualTo(
            TimeUnit.MILLISECONDS.convert(messages.get(99).getTimestamp(), TimeUnit.MICROSECONDS));

    // Can't add messages to current chunk after roll over.
    assertThatThrownBy(
            () ->
                currentChunk.addMessage(SpanUtil.makeSpan(100000), TEST_KAFKA_PARTITION_ID, 100000))
        .isInstanceOf(IllegalStateException.class);

    // Ensure data is cleaned up in the manager
    assertThat(chunkManager.getChunkList()).isEmpty();
    assertThat(chunkManager.getActiveChunk()).isNull();

    // Ensure data on disk is deleted.
    File[] filesAfterRollover = indexDirectory.listFiles();
    assertThat(filesAfterRollover).isNotNull();
    assertThat(filesBeforeRollover.length > filesAfterRollover.length).isTrue();

    chunkManager.stopAsync();
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManager = null;
  }

  private void testChunkManagerSearch(
      ChunkManager<LogMessage> chunkManager, String searchString, int expectedHitCount)
      throws IOException {

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            0,
            TemporaryLogStoreAndSearcherExtension.MAX_TIME,
            10,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder(searchString, 0L, MAX_TIME),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    SearchResult<LogMessage> result = chunkManager.query(searchQuery, Duration.ofMillis(3000));

    assertThat(result.hits.size()).isEqualTo(expectedHitCount);

    // Special case: if we're expecting this search to have no hits then it won't have any
    // snapshots
    // or replicas either
    if (expectedHitCount == 0) {
      assertThat(result.totalSnapshots).isEqualTo(0);
      assertThat(result.snapshotsWithReplicas).isEqualTo(0);
    } else {
      assertThat(result.totalSnapshots).isEqualTo(1);
      assertThat(result.snapshotsWithReplicas).isEqualTo(1);
    }
  }

  // TODO: Add a unit test where the chunk manager uses a different field conflict policy like
  // RAISE_ERROR.

  @Test
  public void testAddMessageWithPropertyTypeConflicts() throws Exception {
    initChunkManager(blobStore);

    // Add a valid message
    int offset = 1;
    Trace.Span msg1 = SpanUtil.makeSpan(1);
    chunkManager.addMessage(msg1, msg1.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;

    // Add an invalid message
    Trace.KeyValue conflictTag =
        Trace.KeyValue.newBuilder()
            .setVInt32(20000)
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();
    Trace.Span msg100 = SpanUtil.makeSpan(100, "Message100", Instant.now(), List.of(conflictTag));
    chunkManager.addMessage(msg100, msg100.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    //noinspection UnusedAssignment
    offset++;

    // Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    testChunkManagerSearch(chunkManager, "Message1", 1);
    testChunkManagerSearch(chunkManager, "Message100", 1);

    // Check metadata.
    List<SnapshotMetadata> snapshots =
        AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(0);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isZero();
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(0);
    List<SearchMetadata> searchNodes = AstraMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(searchNodes).isEmpty();
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .isEmpty();
    assertThat(snapshots.stream().filter(s -> s.endTimeEpochMs == MAX_FUTURE_TIME)).isEmpty();
  }

  // TODO: Add a test to create roll over failure due to ZK.

  @Test
  public void testAddMessagesWithFailedRollOverStopsIngestion() throws Exception {
    // Use a non-existent bucket to induce roll-over failure.
    initChunkManager(new BlobStore(s3AsyncClient, "fakebucket"));

    int offset = 1;

    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 20, 1, Instant.now());
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    assertThat(chunkManager.waitForRollOvers()).isFalse();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(0);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(20);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(1);
    testChunkManagerSearch(chunkManager, "Message1", 0);

    // Ensure can't add messages once roll over is complete.
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    SpanUtil.makeSpan(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    // Check metadata.
    List<SnapshotMetadata> snapshots =
        AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(0);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isZero();
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(0);
    List<SearchMetadata> searchNodes = AstraMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(searchNodes).isEmpty();
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .isEmpty();
    assertThat(snapshots.stream().filter(s -> s.endTimeEpochMs == MAX_FUTURE_TIME)).isEmpty();

    // roll over active chunk on close.
    chunkManager.stopAsync();

    // Can't add messages to a chunk that's shutdown.
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    SpanUtil.makeSpan(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManager = null;
  }
}
