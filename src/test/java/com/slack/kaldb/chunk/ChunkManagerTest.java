package com.slack.kaldb.chunk;

import static com.slack.kaldb.chunk.ChunkManager.LIVE_BYTES_INDEXED;
import static com.slack.kaldb.chunk.ChunkManager.LIVE_MESSAGES_INDEXED;
import static com.slack.kaldb.chunk.RollOverChunkTask.ROLLOVERS_COMPLETED;
import static com.slack.kaldb.chunk.RollOverChunkTask.ROLLOVERS_FAILED;
import static com.slack.kaldb.chunk.RollOverChunkTask.ROLLOVERS_INITIATED;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getValue;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.com.slack.kaldb.logstore.search.AlreadyClosedLogIndexSearcherImpl;
import com.slack.kaldb.com.slack.kaldb.logstore.search.IllegalArgumentLogIndexSearcherImpl;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class ChunkManagerTest {
  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ChunkManager<LogMessage> chunkManager = null;

  private SimpleMeterRegistry metricsRegistry;
  private S3Client s3Client;

  private static final String S3_TEST_BUCKET = "test-kaldb-logs";
  private S3BlobFs s3BlobFs;

  @Before
  public void setUp() throws InvalidProtocolBufferException {
    KaldbConfigUtil.initEmptyIndexerConfig();
    metricsRegistry = new SimpleMeterRegistry();
    // create an S3 client and a bucket for test
    s3Client = S3_MOCK_RULE.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_TEST_BUCKET).build());

    s3BlobFs = new S3BlobFs();
    s3BlobFs.init(s3Client);
  }

  @After
  public void tearDown() {
    metricsRegistry.close();
    if (chunkManager != null) {
      chunkManager.close();
    }
    s3Client.close();
  }

  @Test
  public void testAddMessage() throws IOException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 1000000L);

    final String CHUNK_DATA_PREFIX = "testData";
    chunkManager =
        new ChunkManager<>(
            CHUNK_DATA_PREFIX,
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            MoreExecutors.newDirectExecutorService(),
            3000);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
    int actualChunkSize = 0;
    for (LogMessage m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, 100);
      actualChunkSize += msgSize;
    }
    chunkManager.getActiveChunk().commit();

    assertThat(chunkManager.getChunkMap().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(100);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(100);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(actualChunkSize);

    SearchQuery searchQuery =
        new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000);
    SearchResult<LogMessage> results = chunkManager.query(searchQuery).join();
    assertThat(results.hits.size()).isEqualTo(1);

    // Test chunk metadata.
    ChunkInfo chunkInfo = chunkManager.getActiveChunk().info();
    assertThat(chunkInfo.getChunkSnapshotTimeEpochSecs()).isZero();
    assertThat(chunkInfo.getDataStartTimeEpochSecs()).isGreaterThan(0);
    assertThat(chunkInfo.getDataEndTimeEpochSecs()).isGreaterThan(0);
    assertThat(chunkInfo.chunkId).startsWith(CHUNK_DATA_PREFIX);
    // TODO: Update data ranges based on message.
    // TODO: Test chunk roll over and deletion (life cycle) of a single chunk in this test.
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
    SearchResult<LogMessage> result = chunkManager.query(searchQuery).join();

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
    return chunkManager.query(searchQuery).join().hits.size();
  }

  @Test
  public void testAddAndSearchMessageInMultipleSlices() throws IOException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    final String CHUNK_DATA_PREFIX = "testData";
    chunkManager =
        new ChunkManager<>(
            CHUNK_DATA_PREFIX,
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            MoreExecutors.newDirectExecutorService(),
            3000);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 15);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    chunkManager.getActiveChunk().commit();

    assertThat(chunkManager.getChunkMap().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(15);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message1 OR Message11", 2, 2, 2, 0, MAX_TIME);
  }

  @Test
  public void testAddMessageWithPropertyTypeErrors() throws IOException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    ListeningExecutorService rollOverExecutor = ChunkManager.makeDefaultRollOverExecutor();
    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            rollOverExecutor,
            3000);

    // Add a message
    LogMessage msg1 = MessageUtil.makeMessage(1);
    chunkManager.addMessage(msg1, msg1.toString().length(), 100);

    // Add an invalid message
    LogMessage msg100 = MessageUtil.makeMessage(100);
    MessageUtil.addFieldToMessage(msg100, LogMessage.ReservedField.HOSTNAME.fieldName, 20000);
    chunkManager.addMessage(msg100, msg100.toString().length(), 100);

    // Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();

    assertThat(chunkManager.getChunkMap().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(1);
    testChunkManagerSearch(chunkManager, "Message1", 1, 1, 1, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message100", 0, 1, 1, 0, MAX_TIME);
  }

  @Test(expected = ReadOnlyChunkInsertionException.class)
  public void testMessagesAddedToActiveChunks() throws IOException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 2L);

    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            MoreExecutors.newDirectExecutorService(),
            3000);

    // Add a message
    LogMessage msg1 = MessageUtil.makeMessage(1);
    LogMessage msg2 = MessageUtil.makeMessage(2);

    chunkManager.addMessage(msg1, msg1.toString().length(), 100);
    Chunk<LogMessage> chunk1 = chunkManager.getActiveChunk();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(1);

    chunkManager.addMessage(msg2, msg2.toString().length(), 100);
    assertThat(chunkManager.getChunkMap().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0); // Roll over.
    testChunkManagerSearch(chunkManager, "Message2", 1, 1, 1, 0, MAX_TIME);

    LogMessage msg3 = MessageUtil.makeMessage(3);
    LogMessage msg4 = MessageUtil.makeMessage(4);

    chunkManager.addMessage(msg3, msg3.toString().length(), 100);
    assertThat(chunkManager.getChunkMap().size()).isEqualTo(2);
    Chunk<LogMessage> chunk2 = chunkManager.getActiveChunk();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(1);
    // Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();
    testChunkManagerSearch(chunkManager, "Message3", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);

    // Inserting in an older chunk throws an exception. So, additions go to active chunks only.
    chunk1.addMessage(msg4);
  }

  @Test
  public void testMultiThreadedChunkRollover() throws IOException, InterruptedException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    ListeningExecutorService rollOverExecutor = ChunkManager.makeDefaultRollOverExecutor();
    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            rollOverExecutor,
            3000);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 25);
    // Add 11 messages to initiate first roll over.
    for (LogMessage m : messages.subList(0, 11)) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }

    // Main chunk is already committed. Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();
    // Wait for roll over.
    rollOverExecutor.awaitTermination(10, TimeUnit.SECONDS);

    assertThat(chunkManager.getChunkMap().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 0, 2, 2, 0, MAX_TIME);
  }

  @Test
  public void testAddMessagesToChunkWithRollover() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            MoreExecutors.newDirectExecutorService(),
            3000);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 25);
    // Add 11 messages to initiate first roll over.
    for (LogMessage m : messages.subList(0, 11)) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    // Main chunk is already committed. Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();
    ChunkInfo secondChunk = chunkManager.getActiveChunk().info();
    assertThat(chunkManager.getChunkMap().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);
    // TODO: Test commit and refresh count
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 0, 2, 2, 0, MAX_TIME);

    for (LogMessage m : messages.subList(11, 25)) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    chunkManager.getActiveChunk().commit();
    assertThat(chunkManager.getChunkMap().size()).isEqualTo(3);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(25);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);
    testChunkManagerSearch(chunkManager, "Message1", 1, 3, 3, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 3, 3, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 1, 3, 3, 0, MAX_TIME);

    assertThat(chunkManager.getActiveChunk().info().getChunkSnapshotTimeEpochSecs()).isZero();
    chunkManager.rollOverActiveChunk();
    for (Chunk<LogMessage> c : chunkManager.getChunkMap().values()) {
      assertThat(c.info().getChunkSnapshotTimeEpochSecs()).isGreaterThan(0);
      assertThat(c.info().getDataEndTimeEpochSecs()).isGreaterThan(0);
    }

    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(3);

    // Search all messages.
    for (int i = 1; i <= 25; i++) {
      testChunkManagerSearch(chunkManager, "Message" + i, 1, 3, 3, 0, MAX_TIME);
    }
    // No search results for this query.
    testChunkManagerSearch(chunkManager, "Message261", 0, 3, 3, 0, MAX_TIME);

    testOneFailedChunk(secondChunk);
  }

  public void testOneFailedChunk(ChunkInfo secondChunk) throws Exception {
    Chunk<LogMessage> chunk = chunkManager.getChunkMap().get(secondChunk.chunkId);

    testChunkManagerSearch(chunkManager, "Message18", 1, 3, 3, 0, MAX_TIME);
    // chunk 2 which has docs 12-21 is corrupted
    // an alternate approach I tried was the statement below
    // chunk.getLogSearcher().close();
    // this worked but was kinda flakey since it messes with shutdown and refresh intervals
    chunk.setLogSearcher(new AlreadyClosedLogIndexSearcherImpl());

    testChunkManagerSearch(chunkManager, "Message18", 0, 3, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message1", 1, 3, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message25", 1, 3, 2, 0, MAX_TIME);
  }

  @Test
  public void testAllChunkFailures() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            MoreExecutors.newDirectExecutorService(),
            3000);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 25);
    // Add 11 messages to initiate first roll over.
    for (LogMessage m : messages.subList(0, 11)) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    // Main chunk is already committed. Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();

    assertThat(chunkManager.getChunkMap().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);
    // TODO: Test commit and refresh count
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 0, 2, 2, 0, MAX_TIME);

    for (LogMessage m : messages.subList(11, 25)) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    chunkManager.getActiveChunk().commit();
    assertThat(chunkManager.getChunkMap().size()).isEqualTo(3);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(25);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);
    testChunkManagerSearch(chunkManager, "Message1", 1, 3, 3, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 3, 3, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 1, 3, 3, 0, MAX_TIME);

    chunkManager
        .getChunkMap()
        .values()
        .forEach(chunk -> chunk.setLogSearcher(new AlreadyClosedLogIndexSearcherImpl()));

    testChunkManagerSearch(chunkManager, "Message1", 0, 3, 0, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 0, 3, 0, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message21", 0, 3, 0, 0, MAX_TIME);

    chunkManager
        .getChunkMap()
        .values()
        .forEach(chunk -> chunk.setLogSearcher(new IllegalArgumentLogIndexSearcherImpl()));

    try {
      searchAndGetHitCount(chunkManager, "Message1", 0, MAX_TIME);
      Assert.fail("Should always fail");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testCommitInvalidChunk() throws IOException {
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
    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            MoreExecutors.newDirectExecutorService(),
            3000);

    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }

    assertThat(chunkManager.getChunkMap().size()).isEqualTo(3);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(30);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(3);

    // No new chunk left to commit.
    assertThat(chunkManager.getActiveChunk()).isNull();
  }

  // TODO: Ensure search at ms slices. Currently at sec resolution?

  @Test
  public void testMultiChunkSearch() throws IOException {
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
    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            MoreExecutors.newDirectExecutorService(),
            3000);

    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }

    // Main chunk is already committed. Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();
    assertThat(chunkManager.getChunkMap().size()).isEqualTo(4);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(35);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(3);
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

  @Test(expected = ChunkRollOverInProgressException.class)
  public void testChunkRollOverInProgressExceptionIsThrown() throws IOException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 20, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            ChunkManager.makeDefaultRollOverExecutor(),
            10000);

    // Adding a messages very quickly when running a rollover in background would result in an
    // exception.
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
  }

  @Test
  public void testSuccessfulRollOverFinishesOnClose() throws IOException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            ChunkManager.makeDefaultRollOverExecutor(),
            10000);

    // Adding a message and close the chunkManager right away should still finish the failed
    // rollover.
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    ListenableFuture<?> rollOverFuture = chunkManager.getRolloverFuture();
    chunkManager.close();
    chunkManager = null;

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);
    assertThat(rollOverFuture.isDone()).isTrue();
  }

  @Test
  public void testFailedRollOverFinishesOnClose() throws IOException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET + "Fail", // Missing S3 bucket.
            ChunkManager.makeDefaultRollOverExecutor(),
            10000);

    // Adding a message and close the chunkManager right away should still finish the failed
    // rollover.
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    ListenableFuture<?> rollOverFuture = chunkManager.getRolloverFuture();
    chunkManager.close();
    chunkManager = null;

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);
    assertThat(rollOverFuture.isDone()).isTrue();
  }

  @Test(expected = ChunkRollOverException.class)
  public void testRollOverFailure()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET + "Fail", // Missing S3 bucket.
            ChunkManager.makeDefaultRollOverExecutor(),
            10000);

    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);

    // Adding a message after a roll over fails throws an exception.
    final List<LogMessage> newMessage =
        MessageUtil.makeMessagesWithTimeDifference(11, 12, 1000, startTime);
    for (LogMessage m : newMessage) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
  }

  @Test(expected = ChunkRollOverException.class)
  public void testRollOverFailureWithDirectExecutor()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET + "Fail", // Missing S3 bucket.
            MoreExecutors.newDirectExecutorService(),
            10000);

    // Adding a messages very quickly when running a rollover in background would result in an
    // exception.
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);

    // Adding a message after a roll over fails throws an exception.
    final List<LogMessage> newMessage =
        MessageUtil.makeMessagesWithTimeDifference(11, 12, 1000, startTime);
    for (LogMessage m : newMessage) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
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
    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            ChunkManager.makeDefaultRollOverExecutor(),
            5000);

    List<LogMessage> messages1 = messages.subList(0, 3);
    List<LogMessage> messages2 = messages.subList(3, 6);

    // Add first set of messages, wait for roll over, then add next set of messages.
    insertMessages(chunkManager, messages1, msgsPerChunk);

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);

    insertMessages(chunkManager, messages2, msgsPerChunk);

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(6);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();

    // Main chunk is already committed. Commit the new chunk so we can search it.
    assertThat(chunkManager.getChunkMap().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(6);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);
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
    chunkManager =
        new ChunkManager<>(
            "testData",
            temporaryFolder.newFolder().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            ChunkManager.makeDefaultRollOverExecutor(),
            5000);

    List<LogMessage> messages1 = messages.subList(0, 10);
    List<LogMessage> messages2 = messages.subList(10, 20);

    // Add first set of messages, wait for roll over, then add next set of messages.
    insertMessages(chunkManager, messages1, msgsPerChunk);

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);

    insertMessages(chunkManager, messages2, msgsPerChunk);

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(20);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();

    // Main chunk is already committed. Commit the new chunk so we can search it.
    assertThat(chunkManager.getChunkMap().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(20);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);
  }

  private void insertMessages(
      ChunkManager<LogMessage> chunkManager, List<LogMessage> messages, long msgsPerChunk)
      throws IOException {
    int actualMessagesGauge = 0;
    int actualBytesGauge = 0;
    for (LogMessage m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, 100);
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
