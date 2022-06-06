package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.chunk.ChunkInfo.MAX_FUTURE_TIME;
import static com.slack.kaldb.chunkManager.IndexingChunkManager.LIVE_BYTES_INDEXED;
import static com.slack.kaldb.chunkManager.IndexingChunkManager.LIVE_MESSAGES_INDEXED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.*;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.CHUNK_DATA_PREFIX;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.*;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getValue;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static org.assertj.core.api.Assertions.*;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.*;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class RecoveryChunkManagerTest {
  // TODO: Ensure clean close after all chunks are uploaded.
  // TODO: Test post snapshot for recovery.

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static final String TEST_KAFKA_PARTITION_ID = "10";
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private RecoveryChunkManager<LogMessage> chunkManager = null;

  private SimpleMeterRegistry metricsRegistry;
  private S3Client s3Client;

  private static final String S3_TEST_BUCKET = "test-kaldb-logs";
  private static final String ZK_PATH_PREFIX = "testZK";
  private S3BlobFs s3BlobFs;
  private TestingServer localZkServer;
  private MetadataStore metadataStore;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    // create an S3 client.
    s3Client = S3_MOCK_RULE.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_TEST_BUCKET).build());
    s3BlobFs = new S3BlobFs(s3Client);

    localZkServer = new TestingServer();
    localZkServer.start();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(localZkServer.getConnectString())
            .setZkPathPrefix(ZK_PATH_PREFIX)
            .setZkSessionTimeoutMs(15000)
            .setZkConnectionTimeoutMs(1500)
            .setSleepBetweenRetriesMs(1000)
            .build();

    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(metricsRegistry, zkConfig);
    searchMetadataStore = new SearchMetadataStore(metadataStore, false);
    snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
  }

  @After
  public void tearDown() throws TimeoutException, IOException, InterruptedException {
    metricsRegistry.close();
    if (chunkManager != null) {
      chunkManager.stopAsync();
      chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    metadataStore.close();
    s3Client.close();
    localZkServer.stop();
  }

  private void initChunkManager(String testS3Bucket) throws Exception {

    KaldbConfigs.KaldbConfig kaldbCfg =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:9090",
            9000,
            "testKafkaTopic",
            0,
            "kaldb_test_client",
            testS3Bucket,
            9000 + 1,
            "localhost:2181",
            "recoveryZK_",
            KaldbConfigs.NodeRole.RECOVERY,
            10000,
            "api_log",
            9003);

    chunkManager =
        RecoveryChunkManager.fromConfig(
            metricsRegistry,
            searchMetadataStore,
            snapshotMetadataStore,
            kaldbCfg.getIndexerConfig(),
            s3BlobFs,
            kaldbCfg.getS3Config());
    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void testAddMessageAndRollover() throws Exception {
    initChunkManager(S3_TEST_BUCKET);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
    int actualChunkSize = 0;
    int offset = 1;
    for (LogMessage m : messages) {
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
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(searchMetadataStore.listSync()).isEmpty();

    // Search query
    SearchQuery searchQuery =
        new SearchQuery(
            Collections.emptyList(),
            MessageUtil.TEST_INDEX_NAME,
            "Message1",
            0,
            MAX_TIME,
            10,
            1000);
    SearchResult<LogMessage> results = chunkManager.getActiveChunk().query(searchQuery);
    assertThat(results.hits.size()).isEqualTo(1);

    // Test chunk metadata.
    ChunkInfo chunkInfo = chunkManager.getActiveChunk().info();
    assertThat(chunkInfo.getChunkSnapshotTimeEpochMs()).isZero();
    assertThat(chunkInfo.getDataStartTimeEpochMs()).isGreaterThan(0);
    assertThat(chunkInfo.getDataEndTimeEpochMs()).isGreaterThan(0);
    assertThat(chunkInfo.chunkId).startsWith(CHUNK_DATA_PREFIX);
    assertThat(chunkInfo.getMaxOffset()).isEqualTo(offset - 1);
    assertThat(chunkInfo.getDataStartTimeEpochMs()).isEqualTo(messages.get(0).timeSinceEpochMilli);
    assertThat(chunkInfo.getDataEndTimeEpochMs()).isEqualTo(messages.get(99).timeSinceEpochMilli);

    // Add a message with a very high offset.
    final int veryHighOffset = 1000;
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(offset - 1);
    assertThat(veryHighOffset - offset).isGreaterThan(100);
    LogMessage messageWithHighOffset = MessageUtil.makeMessage(101);
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
                        Collections.emptyList(),
                        MessageUtil.TEST_INDEX_NAME,
                        "Message101",
                        0,
                        MAX_TIME,
                        10,
                        1000))
                .hits
                .size())
        .isEqualTo(1);

    // Add a message with a lower offset.
    final int lowerOffset = 500;
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(veryHighOffset);
    assertThat(lowerOffset - offset).isGreaterThan(100);
    assertThat(veryHighOffset - lowerOffset).isGreaterThan(100);
    LogMessage messageWithLowerOffset = MessageUtil.makeMessage(102);
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
                        Collections.emptyList(),
                        MessageUtil.TEST_INDEX_NAME,
                        "Message102",
                        0,
                        MAX_TIME,
                        10,
                        1000))
                .hits
                .size())
        .isEqualTo(1);

    // Inserting a message from a different kafka partition fails
    LogMessage messageWithInvalidTopic = MessageUtil.makeMessage(103);
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    messageWithInvalidTopic,
                    messageWithInvalidTopic.toString().length(),
                    "differentKafkaTopic",
                    lowerOffset + 1));

    // Roll over chunk.
    assertThat(chunkManager.waitForRollOvers()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(searchMetadataStore.listSync()).isEmpty();
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(snapshots.get(0).startTimeEpochMs).isEqualTo(messages.get(0).timeSinceEpochMilli);
    assertThat(snapshots.get(0).endTimeEpochMs)
        .isGreaterThanOrEqualTo(messages.get(99).timeSinceEpochMilli);

    // Can't add messages to current chunk after roll over.
    assertThatThrownBy(
            () ->
                currentChunk.addMessage(
                    MessageUtil.makeMessage(100000), TEST_KAFKA_PARTITION_ID, 100000))
        .isInstanceOf(IllegalStateException.class);

    // TODO: Ensure data on disk is deleted.
    chunkManager.stopAsync();
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManager = null;
  }

  private void testChunkManagerSearch(
      ChunkManager<LogMessage> chunkManager, String searchString, int expectedHitCount) {

    SearchQuery searchQuery =
        new SearchQuery(
            Collections.emptyList(),
            MessageUtil.TEST_INDEX_NAME,
            searchString,
            0,
            com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME,
            10,
            1000);
    SearchResult<LogMessage> result = chunkManager.query(searchQuery);

    assertThat(result.hits.size()).isEqualTo(expectedHitCount);
    assertThat(result.totalSnapshots).isEqualTo(1);
    assertThat(result.snapshotsWithReplicas).isEqualTo(1);
  }

  @Test
  public void testAddMessageWithPropertyTypeErrors() throws Exception {
    initChunkManager(S3_TEST_BUCKET);

    // Add a valid message
    int offset = 1;
    LogMessage msg1 = MessageUtil.makeMessage(1);
    chunkManager.addMessage(msg1, msg1.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;

    // Add an invalid message
    LogMessage msg100 = MessageUtil.makeMessage(100);
    MessageUtil.addFieldToMessage(msg100, LogMessage.ReservedField.HOSTNAME.fieldName, 20000);
    chunkManager.addMessage(msg100, msg100.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;

    // Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(1);
    testChunkManagerSearch(chunkManager, "Message1", 1);
    testChunkManagerSearch(chunkManager, "Message100", 0);

    // Check metadata.
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    assertThat(snapshots.size()).isEqualTo(0);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isZero();
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(0);
    List<SearchMetadata> searchNodes = searchMetadataStore.listSync();
    assertThat(searchNodes).isEmpty();
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .isEmpty();
    assertThat(snapshots.stream().filter(s -> s.endTimeEpochMs == MAX_FUTURE_TIME)).isEmpty();
  }

  // TODO: Add a test to create roll over failure due to ZK.

  @Test
  public void testAddMessagesWithFailedRollOverStopsIngestion() throws Exception {
    // Use a non-existent bucket to induce roll-over failure.
    initChunkManager("fakebucket");

    int offset = 1;
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 20);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    assertThat(chunkManager.waitForRollOvers()).isFalse();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(20);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(1);
    testChunkManagerSearch(chunkManager, "Message1", 1);
    testChunkManagerSearch(chunkManager, "Message20", 1);

    // Ensure can't add messages once roll over is complete.
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    MessageUtil.makeMessage(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    // Check metadata.
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    assertThat(snapshots.size()).isEqualTo(0);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isZero();
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(0);
    List<SearchMetadata> searchNodes = searchMetadataStore.listSync();
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
                    MessageUtil.makeMessage(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManager = null;
  }
}
