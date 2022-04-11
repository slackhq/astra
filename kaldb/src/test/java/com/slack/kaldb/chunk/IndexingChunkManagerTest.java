package com.slack.kaldb.chunk;

import static com.slack.kaldb.chunk.ChunkInfo.MAX_FUTURE_TIME;
import static com.slack.kaldb.chunkManager.IndexingChunkManager.LIVE_BYTES_INDEXED;
import static com.slack.kaldb.chunkManager.IndexingChunkManager.LIVE_MESSAGES_INDEXED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.*;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.TEST_HOST;
import static com.slack.kaldb.testlib.ChunkManagerUtil.TEST_PORT;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchLiveSnapshot;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchNonLiveSnapshot;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchSearchNodes;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchSnapshots;
import static com.slack.kaldb.testlib.MetricsUtil.*;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunkManager.ChunkCleanerService;
import com.slack.kaldb.chunkManager.ChunkManager;
import com.slack.kaldb.chunkManager.ChunkRollOverException;
import com.slack.kaldb.chunkManager.ChunkRollOverStrategy;
import com.slack.kaldb.chunkManager.ChunkRollOverStrategyImpl;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.com.slack.kaldb.logstore.search.AlreadyClosedLogIndexSearcherImpl;
import com.slack.kaldb.com.slack.kaldb.logstore.search.IllegalArgumentLogIndexSearcherImpl;
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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.assertj.core.data.Offset;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class IndexingChunkManagerTest {

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static final String TEST_KAFKA_PARTITION_ID = "10";
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private IndexingChunkManager<LogMessage> chunkManager = null;

  private SimpleMeterRegistry metricsRegistry;
  private S3Client s3Client;

  private static final String S3_TEST_BUCKET = "test-kaldb-logs";
  private static final String ZK_PATH_PREFIX = "testZK";
  private S3BlobFs s3BlobFs;
  private TestingServer localZkServer;
  private MetadataStore metadataStore;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    // create an S3 client and a bucket for test
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
  }

  @After
  public void tearDown() throws TimeoutException, IOException {
    metricsRegistry.close();
    if (chunkManager != null) {
      chunkManager.stopAsync();
      chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    metadataStore.close();
    s3Client.close();
    localZkServer.stop();
  }

  private void initChunkManager(
      ChunkRollOverStrategy chunkRollOverStrategy,
      String s3TestBucket,
      ListeningExecutorService listeningExecutorService,
      int rollOverFutureTimeoutMs)
      throws IOException, TimeoutException {
    SearchContext searchContext = new SearchContext(TEST_HOST, TEST_PORT);
    chunkManager =
        new IndexingChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            s3TestBucket,
            listeningExecutorService,
            rollOverFutureTimeoutMs,
            metadataStore,
            searchContext,
            KaldbConfigUtil.makeIndexerConfig(TEST_PORT, 1000, "log_message", 100));
    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @Test
  @Ignore
  // Todo: this test needs to be refactored as it currently does not reliably replicate the race
  //   condition Additionally, this test as currently written is extremely slow, and accounts
  //   for over 10% of our test runtime
  public void closeDuringCleanerTask()
      throws IOException, TimeoutException, ExecutionException, InterruptedException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 1000000L);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService(), 3000);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 10);
    int offset = 1;
    for (LogMessage m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      offset++;
      chunkManager.getActiveChunk().commit();

      // force creation of a unique chunk for every message
      chunkManager.rollOverActiveChunk();
    }
    assertThat(chunkManager.getChunkList().size()).isEqualTo(10);

    // attempt to clean all chunks while shutting the service down
    // we use an executor service since the chunkCleaner is an AbstractScheduledService and we want
    // these to run immediately
    ChunkCleanerService<LogMessage> chunkCleanerService =
        new ChunkCleanerService<>(chunkManager, Duration.ZERO);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<?> cleanerTask = executorService.submit(() -> chunkCleanerService.runAt(Instant.now()));

    chunkManager.stopAsync();
    // wait for both to be complete
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    cleanerTask.get(10, TimeUnit.SECONDS);

    assertThat(chunkManager.getChunkList().size()).isEqualTo(0);
  }

  @Test
  public void testAddMessage()
      throws IOException, TimeoutException, ExecutionException, InterruptedException {
    final Instant creationTime = Instant.now();
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 1000000L);

    final String CHUNK_DATA_PREFIX = "testData";
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService(), 3000);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
    int actualChunkSize = 0;
    int offset = 1;
    for (LogMessage m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      actualChunkSize += msgSize;
      offset++;
    }
    chunkManager.getActiveChunk().commit();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(100);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(100);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(actualChunkSize);

    // Check metadata registration.
    assertThat(chunkManager.getSnapshotMetadataStore().list().get().size()).isEqualTo(1);
    assertThat(chunkManager.getSearchMetadataStore().list().get().size()).isEqualTo(1);

    SearchQuery searchQuery =
        new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000);
    SearchResult<LogMessage> results = chunkManager.query(searchQuery);
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

    List<SnapshotMetadata> snapshots = fetchSnapshots(chunkManager);
    assertThat(snapshots.size()).isEqualTo(1);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isEqualTo(1);
    assertThat(fetchNonLiveSnapshot(snapshots)).isEmpty();
    assertThat(snapshots.get(0).snapshotPath).startsWith(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(snapshots.get(0).maxOffset).isEqualTo(0);
    assertThat(snapshots.get(0).partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(snapshots.get(0).snapshotId).startsWith(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(snapshots.get(0).startTimeEpochMs)
        .isCloseTo(creationTime.toEpochMilli(), Offset.offset(1000L));
    assertThat(snapshots.get(0).endTimeEpochMs).isEqualTo(MAX_FUTURE_TIME);

    List<SearchMetadata> searchNodes = fetchSearchNodes(chunkManager);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.get(0).url).contains(TEST_HOST);
    assertThat(searchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
    assertThat(searchNodes.get(0).snapshotName).contains(SnapshotMetadata.LIVE_SNAPSHOT_PATH);

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
                .query(
                    new SearchQuery(
                        MessageUtil.TEST_INDEX_NAME, "Message101", 0, MAX_TIME, 10, 1000))
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
                .query(
                    new SearchQuery(
                        MessageUtil.TEST_INDEX_NAME, "Message102", 0, MAX_TIME, 10, 1000))
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
  }

  private void testChunkManagerSearch(
      ChunkManager<LogMessage> chunkManager,
      String searchString,
      int expectedHitCount,
      int totalSnapshots,
      int expectedSnapshotsWithReplicas,
      long startTimeEpochMs,
      long endTimeEpochMs) {

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_INDEX_NAME, searchString, startTimeEpochMs, endTimeEpochMs, 10, 1000);
    SearchResult<LogMessage> result = chunkManager.query(searchQuery);

    assertThat(result.hits.size()).isEqualTo(expectedHitCount);
    assertThat(result.totalSnapshots).isEqualTo(totalSnapshots);
    assertThat(result.snapshotsWithReplicas).isEqualTo(expectedSnapshotsWithReplicas);
  }

  private int searchAndGetHitCount(
      ChunkManager<LogMessage> chunkManager,
      String searchString,
      long startTimeEpochMs,
      long endTimeEpochMs) {
    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_INDEX_NAME, searchString, startTimeEpochMs, endTimeEpochMs, 10, 1000);
    return chunkManager.query(searchQuery).hits.size();
  }

  @Test
  public void testAddAndSearchMessageInMultipleSlices()
      throws IOException, TimeoutException, ExecutionException, InterruptedException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService(), 3000);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 15);
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(15);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message1 OR Message11", 2, 2, 2, 0, MAX_TIME);

    checkMetadata(3, 2, 1, 2, 1);
  }

  @Test
  public void testAddMessageWithPropertyTypeErrors()
      throws IOException, TimeoutException, ExecutionException, InterruptedException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    ListeningExecutorService rollOverExecutor = IndexingChunkManager.makeDefaultRollOverExecutor();
    initChunkManager(chunkRollOverStrategy, S3_TEST_BUCKET, rollOverExecutor, 3000);

    // Add a message
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
    testChunkManagerSearch(chunkManager, "Message1", 1, 1, 1, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message100", 0, 1, 1, 0, MAX_TIME);

    // Check metadata.
    checkMetadata(1, 1, 0, 1, 1);
  }

  private void checkMetadata(
      int expectedSnapshotSize,
      int expectedLiveSnapshotSize,
      int expectedNonLiveSnapshotSize,
      int expectedSearchNodeSize,
      int expectedInfinitySnapshotsCount)
      throws InterruptedException, ExecutionException, TimeoutException {
    List<SnapshotMetadata> snapshots = fetchSnapshots(chunkManager);
    assertThat(snapshots.size()).isEqualTo(expectedSnapshotSize);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isEqualTo(expectedLiveSnapshotSize);
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(expectedNonLiveSnapshotSize);
    List<SearchMetadata> searchNodes = fetchSearchNodes(chunkManager);
    assertThat(searchNodes.size()).isEqualTo(expectedSearchNodeSize);
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            searchNodes.stream().map(s -> s.snapshotName).collect(Collectors.toList()));
    assertThat(snapshots.stream().filter(s -> s.endTimeEpochMs == MAX_FUTURE_TIME).count())
        .isEqualTo(expectedInfinitySnapshotsCount);
  }

  @Test(expected = IllegalStateException.class)
  public void testMessagesAddedToActiveChunks()
      throws IOException, TimeoutException, ExecutionException, InterruptedException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 2L);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService(), 3000);

    // Add a message
    List<LogMessage> msgs = MessageUtil.makeMessagesWithTimeDifference(1, 4, 1000);
    LogMessage msg1 = msgs.get(0);
    LogMessage msg2 = msgs.get(1);
    int offset = 1;
    chunkManager.addMessage(msg1, msg1.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;
    ReadWriteChunk<LogMessage> chunk1 = chunkManager.getActiveChunk();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(1);

    chunkManager.addMessage(msg2, msg2.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0); // Roll over.
    testChunkManagerSearch(chunkManager, "Message2", 1, 1, 1, 0, MAX_TIME);

    // Wait for roll over to complete.
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);

    checkMetadata(2, 1, 1, 1, 0);

    LogMessage msg3 = msgs.get(2);
    LogMessage msg4 = msgs.get(3);
    chunkManager.addMessage(msg3, msg3.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);

    chunkManager.getActiveChunk();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(1);
    // Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();
    testChunkManagerSearch(chunkManager, "Message3", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);

    checkMetadata(3, 2, 1, 2, 1);
    // Inserting in an older chunk throws an exception. So, additions go to active chunks only.
    chunk1.addMessage(msg4, TEST_KAFKA_PARTITION_ID, 1);
  }

  @Test
  public void testMultiThreadedChunkRollover()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    ListeningExecutorService rollOverExecutor = IndexingChunkManager.makeDefaultRollOverExecutor();
    initChunkManager(chunkRollOverStrategy, S3_TEST_BUCKET, rollOverExecutor, 3000);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 25);
    // Add 11 messages to initiate first roll over.
    int offset = 1;
    for (LogMessage m : messages.subList(0, 11)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    // Main chunk is already committed. Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();
    // Wait for roll over.
    rollOverExecutor.awaitTermination(10, TimeUnit.SECONDS);

    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 0, 2, 2, 0, MAX_TIME);
    checkMetadata(3, 2, 1, 2, 1);
  }

  @Test
  public void testAddMessagesToChunkWithRollover() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService(), 3000);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 25);
    // Add 11 messages to initiate first roll over.
    int offset = 1;
    for (LogMessage m : messages.subList(0, 11)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    // Main chunk is already committed. Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    checkMetadata(3, 2, 1, 2, 1);
    ChunkInfo secondChunk = chunkManager.getActiveChunk().info();
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    checkMetadata(3, 2, 1, 2, 1);
    // TODO: Test commit and refresh count
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 0, 2, 2, 0, MAX_TIME);

    for (LogMessage m : messages.subList(11, 25)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 2);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(25);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    testChunkManagerSearch(chunkManager, "Message1", 1, 3, 3, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 3, 3, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 1, 3, 3, 0, MAX_TIME);
    checkMetadata(5, 3, 2, 3, 1);

    assertThat(chunkManager.getActiveChunk().info().getChunkSnapshotTimeEpochMs()).isZero();
    chunkManager.rollOverActiveChunk();
    for (Chunk<LogMessage> c : chunkManager.getChunkList()) {
      assertThat(c.info().getChunkSnapshotTimeEpochMs()).isGreaterThan(0);
      assertThat(c.info().getDataEndTimeEpochMs()).isGreaterThan(0);
    }

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 3);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);

    // Search all messages.
    for (int i = 1; i <= 25; i++) {
      testChunkManagerSearch(chunkManager, "Message" + i, 1, 3, 3, 0, MAX_TIME);
    }
    // No search results for this query.
    testChunkManagerSearch(chunkManager, "Message261", 0, 3, 3, 0, MAX_TIME);

    checkMetadata(6, 3, 3, 3, 0);
    testOneFailedChunk(secondChunk);
  }

  private void testOneFailedChunk(ChunkInfo secondChunk) {
    ReadWriteChunk<LogMessage> chunk =
        (ReadWriteChunk<LogMessage>)
            chunkManager
                .getChunkList()
                .stream()
                .filter(chunkIterator -> Objects.equals(chunkIterator.id(), secondChunk.chunkId))
                .findFirst()
                .get();

    testChunkManagerSearch(chunkManager, "Message18", 1, 3, 3, 0, MAX_TIME);
    // chunk 2 which has docs 12-21 is corrupted
    // an alternate approach I tried was the statement below
    // chunk.getLogSearcher().close();
    // this worked but was kinda flaky since it messes with shutdown and refresh intervals
    chunk.setLogSearcher(new AlreadyClosedLogIndexSearcherImpl());

    testChunkManagerSearch(chunkManager, "Message18", 0, 3, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message1", 1, 3, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message25", 1, 3, 2, 0, MAX_TIME);
  }

  @Test
  public void testAllChunkFailures() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService(), 3000);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 25);
    // Add 11 messages to initiate first roll over.
    int offset = 1;
    for (LogMessage m : messages.subList(0, 11)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    // Main chunk is already committed. Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    checkMetadata(3, 2, 1, 2, 1);
    // TODO: Test commit and refresh count
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 0, 2, 2, 0, MAX_TIME);

    for (LogMessage m : messages.subList(11, 25)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 2);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(25);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    checkMetadata(5, 3, 2, 3, 1);
    testChunkManagerSearch(chunkManager, "Message1", 1, 3, 3, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 3, 3, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 1, 3, 3, 0, MAX_TIME);

    chunkManager
        .getChunkList()
        .forEach(
            chunk ->
                ((ReadWriteChunk) chunk).setLogSearcher(new AlreadyClosedLogIndexSearcherImpl()));

    testChunkManagerSearch(chunkManager, "Message1", 0, 3, 0, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 0, 3, 0, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 0, 3, 0, 0, MAX_TIME);

    chunkManager
        .getChunkList()
        .forEach(
            chunk ->
                ((ReadWriteChunk) chunk).setLogSearcher(new IllegalArgumentLogIndexSearcherImpl()));

    Throwable throwable =
        catchThrowable(() -> searchAndGetHitCount(chunkManager, "Message1", 0, MAX_TIME));
    assertThat(Throwables.getRootCause(throwable)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCommitInvalidChunk()
      throws IOException, TimeoutException, ExecutionException, InterruptedException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);
    messages.addAll(
        MessageUtil.makeMessagesWithTimeDifference(
            11, 20, 1000, startTime.plus(2, ChronoUnit.HOURS)));
    messages.addAll(
        MessageUtil.makeMessagesWithTimeDifference(
            21, 30, 1000, startTime.plus(4, ChronoUnit.HOURS)));

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService(), 3000);

    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 3);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(30);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    checkMetadata(6, 3, 3, 3, 0);

    // No new chunk left to commit.
    assertThat(chunkManager.getActiveChunk()).isNull();
  }

  // TODO: Ensure search at ms slices. Currently at sec resolution?

  @Test
  public void testMultiChunkSearch()
      throws IOException, TimeoutException, ExecutionException, InterruptedException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);
    messages.addAll(
        MessageUtil.makeMessagesWithTimeDifference(
            11, 20, 1000, startTime.plus(2, ChronoUnit.HOURS)));
    messages.addAll(
        MessageUtil.makeMessagesWithTimeDifference(
            21, 30, 1000, startTime.plus(4, ChronoUnit.HOURS)));
    messages.addAll(
        MessageUtil.makeMessagesWithTimeDifference(
            31, 35, 1000, startTime.plus(6, ChronoUnit.HOURS)));

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService(), 3000);

    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    // Main chunk is already committed. Commit the new chunk so we can search it.
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 3);
    chunkManager.getActiveChunk().commit();
    assertThat(chunkManager.getChunkList().size()).isEqualTo(4);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(35);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    checkMetadata(7, 4, 3, 4, 1);
    // TODO: Test commit and refresh count

    final long messagesStartTimeMs = messages.get(0).timeSinceEpochMilli;

    // Search all messages
    for (int i = 1; i <= 35; i++) {
      assertThat(searchAndGetHitCount(chunkManager, "Message" + i, 0, MAX_TIME)).isEqualTo(1);
    }

    // 0 to MAX_TIME
    assertThat(searchAndGetHitCount(chunkManager, "Message1 OR Message25", 0, MAX_TIME))
        .isEqualTo(2);

    assertThat(searchAndGetHitCount(chunkManager, "Message2", messagesStartTimeMs, MAX_TIME))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message1", messagesStartTimeMs, MAX_TIME))
        .isEqualTo(1);

    // Message1 & chunk 1
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + 10000;
    // TODO: test chunk metadata.
    assertThat(searchAndGetHitCount(chunkManager, "Message1", chunk1StartTimeMs, chunk1EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message1", chunk1StartTimeMs, chunk1EndTimeMs))
        .isEqualTo(1);

    assertThat(
            searchAndGetHitCount(
                chunkManager, "Message1", chunk1StartTimeMs + 2000, chunk1EndTimeMs))
        .isEqualTo(0);

    assertThat(
            searchAndGetHitCount(
                chunkManager, "Message10", chunk1StartTimeMs + 2000, chunk1EndTimeMs))
        .isEqualTo(1);

    // Message 11 & chunk 2
    assertThat(
            searchAndGetHitCount(
                chunkManager, "Message11", messagesStartTimeMs, messagesStartTimeMs + 10000))
        .isEqualTo(0);

    final long chunk2StartTimeMs = chunk1StartTimeMs + Duration.ofHours(2).toMillis();
    final long chunk2EndTimeMs = chunk2StartTimeMs + 10000;

    assertThat(searchAndGetHitCount(chunkManager, "Message11", chunk2StartTimeMs, chunk2EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message20", chunk2StartTimeMs, chunk2EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message21", chunk2StartTimeMs, chunk2EndTimeMs))
        .isEqualTo(0);

    // Chunk 3
    final long chunk3StartTimeMs = chunk1StartTimeMs + Duration.ofHours(4).toMillis();
    final long chunk3EndTimeMs = chunk3StartTimeMs + 10000;

    assertThat(searchAndGetHitCount(chunkManager, "Message21", chunk3StartTimeMs, chunk3EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message30", chunk3StartTimeMs, chunk3EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message31", chunk3StartTimeMs, chunk3EndTimeMs))
        .isEqualTo(0);

    // Chunk 4
    final long chunk4StartTimeMs = chunk1StartTimeMs + Duration.ofHours(6).toMillis();
    final long chunk4EndTimeMs = chunk4StartTimeMs + 10000;

    assertThat(searchAndGetHitCount(chunkManager, "Message31", chunk4StartTimeMs, chunk4EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message35", chunk4StartTimeMs, chunk4EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message36", chunk4StartTimeMs, chunk4EndTimeMs))
        .isEqualTo(0);

    // TODO: Test the entire search response in all queries and not just hits.
  }

  @Test
  public void testChunkRollOverInProgressExceptionIsThrown()
      throws IOException, TimeoutException, ExecutionException, InterruptedException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 20, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy,
        S3_TEST_BUCKET,
        IndexingChunkManager.makeDefaultRollOverExecutor(),
        10000);

    assertThat(fetchSnapshots(chunkManager)).isEmpty();
    assertThat(fetchLiveSnapshot(fetchSnapshots(chunkManager))).isEmpty();
    assertThat(fetchNonLiveSnapshot(fetchSnapshots(chunkManager))).isEmpty();
    assertThat(fetchSearchNodes(chunkManager)).isEmpty();

    // Adding a messages very quickly when running a rollover in background would result in an
    // exception.
    try {
      int offset = 1;
      for (LogMessage m : messages) {
        chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      fail("Should throw exception");
    } catch (Exception e) {
      assertThat(e).isInstanceOf(ChunkRollOverException.class);
    }

    List<SnapshotMetadata> snapshots = fetchSnapshots(chunkManager);
    assertThat(snapshots.size()).isEqualTo(2);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isGreaterThanOrEqualTo(2);
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(0);
    List<SearchMetadata> searchNodes = fetchSearchNodes(chunkManager);
    assertThat(searchNodes.size()).isEqualTo(2);
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            searchNodes.stream().map(s -> s.snapshotName).collect(Collectors.toList()));
    assertThat(snapshots.stream().filter(s -> s.endTimeEpochMs == MAX_FUTURE_TIME).count())
        .isEqualTo(2);
  }

  @Test
  public void testSuccessfulRollOverFinishesOnClose() throws Exception {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy,
        S3_TEST_BUCKET,
        IndexingChunkManager.makeDefaultRollOverExecutor(),
        10000);

    // Adding a message and close the chunkManager right away should still finish the failed
    // rollover.
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    ListenableFuture<?> rollOverFuture = chunkManager.getRolloverFuture();

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    checkMetadata(2, 1, 1, 1, 0);
    chunkManager.stopAsync();
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManager = null;

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(rollOverFuture.isDone()).isTrue();

    // The stores are closed so temporarily re-create them so we can query the data in ZK.
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(metadataStore, false);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    assertThat(searchMetadataStore.listSync()).isEmpty();
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(1);
    assertThat(fetchLiveSnapshot(snapshots)).isEmpty();
    assertThat(snapshots.get(0).maxOffset).isEqualTo(offset - 1);
    assertThat(snapshots.get(0).endTimeEpochMs).isLessThan(MAX_FUTURE_TIME);
    assertThat(snapshots.get(0).snapshotId).doesNotContain(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    searchMetadataStore.close();
    snapshotMetadataStore.close();
  }

  @Test
  public void testFailedRollOverFinishesOnClose() throws Exception {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy,
        S3_TEST_BUCKET + "Fail",
        IndexingChunkManager.makeDefaultRollOverExecutor(),
        10000);

    // Adding a message and close the chunkManager right away should still finish the failed
    // rollover.
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    await().until(() -> getCount(ROLLOVERS_FAILED, metricsRegistry) == 1);
    checkMetadata(1, 1, 0, 1, 1);
    ListenableFuture<?> rollOverFuture = chunkManager.getRolloverFuture();
    chunkManager.stopAsync();
    chunkManager.awaitTerminated(15, TimeUnit.SECONDS);
    chunkManager = null;

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);
    assertThat(rollOverFuture.isDone()).isTrue();

    // The stores are closed so temporarily re-create them so we can query the data in ZK.
    // All ephemeral data is ZK is deleted and no data or metadata is persisted.
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(metadataStore, false);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    assertThat(searchMetadataStore.listSync()).isEmpty();
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    // TODO: Data is lost and the indexer should use a recovery protocol to re-index this data.
  }

  @Test
  public void testRollOverFailure()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy,
        S3_TEST_BUCKET + "Fail",
        IndexingChunkManager.makeDefaultRollOverExecutor(),
        10000);

    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    await().until(() -> getCount(ROLLOVERS_FAILED, metricsRegistry) == 1);
    assertThat(getTimerCount(ROLLOVER_TIMER, metricsRegistry)).isEqualTo(1);
    checkMetadata(1, 1, 0, 1, 1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);

    // Adding a message after a rollover fails throws an exception.
    final List<LogMessage> newMessage =
        MessageUtil.makeMessagesWithTimeDifference(11, 12, 1000, startTime);
    try {
      for (LogMessage m : newMessage) {
        chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      fail("Should have thrown chunk roll over exception");
    } catch (Exception e) {
      assertThat(e).isInstanceOf(ChunkRollOverException.class);
    }
    checkMetadata(1, 1, 0, 1, 1);
  }

  @Test
  public void testRollOverFailureWithDirectExecutor()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy,
        S3_TEST_BUCKET + "Fail",
        MoreExecutors.newDirectExecutorService(),
        10000);

    // Adding a messages very quickly when running a rollover in background would result in an
    // exception.
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    await().until(() -> getCount(ROLLOVERS_FAILED, metricsRegistry) == 1);
    checkMetadata(1, 1, 0, 1, 1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);

    // Adding a message after a rollover fails throws an exception.
    final List<LogMessage> newMessage =
        MessageUtil.makeMessagesWithTimeDifference(11, 12, 1000, startTime);
    try {
      for (LogMessage m : newMessage) {
        chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      fail("Should have throw a chunk roll over exception");
    } catch (Exception e) {
      assertThat(e).isInstanceOf(ChunkRollOverException.class);
    }
    checkMetadata(1, 1, 0, 1, 1);
  }

  @Test
  public void testMultipleByteRollOversSuccessfully()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 6, 1000, startTime);

    final long msgsPerChunk = 3L;
    final long maxBytesPerChunk = 100L;
    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(maxBytesPerChunk, msgsPerChunk);
    initChunkManager(
        chunkRollOverStrategy,
        S3_TEST_BUCKET,
        IndexingChunkManager.makeDefaultRollOverExecutor(),
        5000);

    List<LogMessage> messages1 = messages.subList(0, 3);
    List<LogMessage> messages2 = messages.subList(3, 6);

    // Add first set of messages, wait for roll over, then add next set of messages.
    insertMessages(chunkManager, messages1, msgsPerChunk);

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    checkMetadata(2, 1, 1, 1, 0);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);

    insertMessages(chunkManager, messages2, msgsPerChunk);

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 2);
    checkMetadata(4, 2, 2, 2, 0);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(6);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();

    // Main chunk is already committed. Commit the new chunk so we can search it.
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(6);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
  }

  @Test
  public void testMultipleCountRollOversSuccessfully()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 20, 1000, startTime);

    final long msgsPerChunk = 10L;
    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, msgsPerChunk);
    initChunkManager(
        chunkRollOverStrategy,
        S3_TEST_BUCKET,
        IndexingChunkManager.makeDefaultRollOverExecutor(),
        5000);

    List<LogMessage> messages1 = messages.subList(0, 10);
    List<LogMessage> messages2 = messages.subList(10, 20);

    // Add first set of messages, wait for roll over, then add next set of messages.
    insertMessages(chunkManager, messages1, msgsPerChunk);

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    checkMetadata(2, 1, 1, 1, 0);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);

    insertMessages(chunkManager, messages2, msgsPerChunk);

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 2);
    checkMetadata(4, 2, 2, 2, 0);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(20);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();

    // Main chunk is already committed. Commit the new chunk so we can search it.
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(20);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);
    assertThat(getTimerCount(ROLLOVER_TIMER, metricsRegistry)).isEqualTo(2);
  }

  private void insertMessages(
      IndexingChunkManager<LogMessage> chunkManager, List<LogMessage> messages, long msgsPerChunk)
      throws IOException {
    int actualMessagesGauge = 0;
    int actualBytesGauge = 0;
    int offset = 1;
    for (LogMessage m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      offset++;
      actualMessagesGauge++;
      actualBytesGauge += msgSize;
      if (actualMessagesGauge < msgsPerChunk) {
        assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(actualMessagesGauge);
        assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(actualBytesGauge);
      } else { // Gauge is reset on roll over
        assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
        assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
      }
    }
  }
}
