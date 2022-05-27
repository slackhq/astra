package com.slack.kaldb.chunk;

import static com.slack.kaldb.chunk.ReadWriteChunk.INDEX_FILES_UPLOAD;
import static com.slack.kaldb.chunk.ReadWriteChunk.INDEX_FILES_UPLOAD_FAILED;
import static com.slack.kaldb.chunk.ReadWriteChunk.LIVE_SNAPSHOT_PREFIX;
import static com.slack.kaldb.chunk.ReadWriteChunk.SNAPSHOT_TIMER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getTimerCount;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@RunWith(Enclosed.class)
public class IndexingChunkImplTest {
  private static final String TEST_KAFKA_PARTITION_ID = "10";
  private static final String TEST_HOST = "localhost";
  private static final int TEST_PORT = 34567;
  private static final String CHUNK_DATA_PREFIX = "testDataSet";
  private static final Duration COMMIT_INTERVAL = Duration.ofSeconds(5 * 60);
  private static final Duration REFRESH_INTERVAL = Duration.ofSeconds(5 * 60);

  private static void testBeforeSnapshotState(
      SnapshotMetadataStore snapshotMetadataStore,
      SearchMetadataStore searchMetadataStore,
      ReadWriteChunk<LogMessage> chunk)
      throws ExecutionException, InterruptedException, TimeoutException {
    assertThat(snapshotMetadataStore.list().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS))
        .containsOnly(ChunkInfo.toSnapshotMetadata(chunk.info(), LIVE_SNAPSHOT_PREFIX));
    final List<SearchMetadata> beforeSearchNodes =
        searchMetadataStore.list().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    assertThat(beforeSearchNodes.size()).isEqualTo(1);
    assertThat(beforeSearchNodes.get(0).url).contains(TEST_HOST);
    assertThat(beforeSearchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
    assertThat(beforeSearchNodes.get(0).snapshotName).contains(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
  }

  public static class BasicTests {
    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private boolean closeChunk = true;
    private MeterRegistry registry;
    private ReadWriteChunk<LogMessage> chunk;
    private TestingServer testingServer;
    private MetadataStore metadataStore;

    @Before
    public void setUp() throws Exception {
      Tracing.newBuilder().build();

      testingServer = new TestingServer();
      KaldbConfigs.ZookeeperConfig zkConfig =
          KaldbConfigs.ZookeeperConfig.newBuilder()
              .setZkConnectString(testingServer.getConnectString())
              .setZkPathPrefix("shouldHandleChunkLivecycle")
              .setZkSessionTimeoutMs(1000)
              .setZkConnectionTimeoutMs(1000)
              .setSleepBetweenRetriesMs(1000)
              .build();

      registry = new SimpleMeterRegistry();

      metadataStore = ZookeeperMetadataStoreImpl.fromConfig(registry, zkConfig);

      SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
      SearchMetadataStore searchMetadataStore = new SearchMetadataStore(metadataStore, true);

      final LuceneIndexStoreImpl logStore =
          LuceneIndexStoreImpl.makeLogStore(
              temporaryFolder.newFolder(), COMMIT_INTERVAL, REFRESH_INTERVAL, registry);
      chunk =
          new IndexingChunkImpl<>(
              logStore,
              CHUNK_DATA_PREFIX,
              registry,
              searchMetadataStore,
              snapshotMetadataStore,
              new SearchContext(TEST_HOST, TEST_PORT),
              TEST_KAFKA_PARTITION_ID);

      chunk.postCreate();
      closeChunk = true;
      testBeforeSnapshotState(snapshotMetadataStore, searchMetadataStore, chunk);
    }

    @After
    public void tearDown() throws IOException, TimeoutException {
      if (closeChunk) chunk.close();

      metadataStore.close();
      testingServer.close();
      registry.close();
    }

    @Test
    public void testAddAndSearchChunk() {
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      int offset = 1;
      for (LogMessage m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      chunk.commit();

      SearchResult<LogMessage> results =
          chunk.query(new SearchQuery(MessageUtil.TEST_INDEX_NAME, "*:*", 0, MAX_TIME, 10, 1000));
      assertThat(results.totalCount).isEqualTo(100);

      results =
          chunk.query(
              new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000));
      assertThat(results.totalCount).isEqualTo(1);

      results =
          chunk.query(
              new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message*", 0, MAX_TIME, 10, 1000));
      assertThat(results.totalCount).isEqualTo(100);
      assertThat(results.hits.size()).isEqualTo(10);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(1);
    }

    @Test
    public void testAddInvalidMessagesToChunk() {
      LogMessage testMessage = MessageUtil.makeMessage(0);
      testMessage.addProperty("username", 0);

      // An Invalid message is dropped but failure counter is incremented.
      chunk.addMessage(testMessage, TEST_KAFKA_PARTITION_ID, 1);
      chunk.commit();

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(1);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(1);
    }

    @Test
    public void testAddAndSearchChunkInTimeRange() {
      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
      final List<LogMessage> messages =
          MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
      final long messageStartTimeMs = messages.get(0).timeSinceEpochMilli;
      int offset = 1;
      for (LogMessage m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      chunk.commit();

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(1);

      final long expectedEndTimeEpochMs = messageStartTimeMs + (99 * 1000);
      // Ensure chunk info is correct.
      assertThat(chunk.info().getDataStartTimeEpochMs()).isEqualTo(messageStartTimeMs);
      assertThat(chunk.info().getDataEndTimeEpochMs()).isEqualTo(expectedEndTimeEpochMs);
      assertThat(chunk.info().chunkId).contains(CHUNK_DATA_PREFIX);
      assertThat(chunk.info().getChunkSnapshotTimeEpochMs()).isZero();
      assertThat(chunk.info().getChunkCreationTimeEpochMs()).isPositive();

      // Search for message in expected time range.
      searchChunk("Message1", messageStartTimeMs, expectedEndTimeEpochMs, 1);

      // Search for message before and after the time range.
      searchChunk("Message1", 0, messageStartTimeMs - 1000, 0);
      searchChunk("Message1", expectedEndTimeEpochMs + 1000, MAX_TIME, 0);

      // Search for Message1 in time range.
      searchChunk("Message1", 0, messageStartTimeMs, 1);
      searchChunk("Message100", 0, messageStartTimeMs, 0);

      // Search for Message100 in time range.
      searchChunk("Message100", messageStartTimeMs, expectedEndTimeEpochMs, 1);

      // Message100 is in chunk but not in time range.
      searchChunk("Message100", messageStartTimeMs, messageStartTimeMs + 1000, 0);

      // Add more messages in other time range and search again with new time ranges.
      final List<LogMessage> newMessages =
          MessageUtil.makeMessagesWithTimeDifference(
              1, 100, 1000, startTime.plus(2, ChronoUnit.DAYS));
      final long newMessageStartTimeEpochMs = newMessages.get(0).timeSinceEpochMilli;
      for (LogMessage m : newMessages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      chunk.commit();

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(200);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(2);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(2);

      assertThat(chunk.info().getDataStartTimeEpochMs()).isEqualTo(messageStartTimeMs);
      assertThat(chunk.info().getDataEndTimeEpochMs())
          .isEqualTo(newMessageStartTimeEpochMs + (99 * 1000));

      // Search for message in expected time range.
      searchChunk("Message1", messageStartTimeMs, expectedEndTimeEpochMs, 1);

      // Search for message before and after the time range.
      searchChunk("Message1", 0, messageStartTimeMs - 1000, 0);

      // Search for Message1 in time range.
      searchChunk("Message1", 0, messageStartTimeMs, 1);

      // Search for Message100 in time range.
      searchChunk("Message100", messageStartTimeMs, expectedEndTimeEpochMs, 1);

      // Message100 is in chunk but not in time range.
      searchChunk("Message100", messageStartTimeMs, messageStartTimeMs + 1000, 0);

      // Search for new and old messages
      searchChunk("Message1", messageStartTimeMs + 1000, MAX_TIME, 1);
      searchChunk("Message1", messageStartTimeMs, newMessageStartTimeEpochMs + (100 * 1000), 2);
      searchChunk("Message1", messageStartTimeMs, MAX_TIME, 2);

      // Search for Message100 in time range.
      searchChunk("Message100", messageStartTimeMs, newMessageStartTimeEpochMs + (100 * 1000), 2);

      // Message100 is in chunk but not in time range.
      searchChunk("Message100", newMessageStartTimeEpochMs + (100 * 1000), MAX_TIME, 0);
    }

    private void searchChunk(
        String searchString, long startTimeMs, long endTimeMs, int expectedResultCount) {
      assertThat(
              chunk
                  .query(
                      new SearchQuery(
                          MessageUtil.TEST_INDEX_NAME,
                          searchString,
                          startTimeMs,
                          endTimeMs,
                          10,
                          1000))
                  .hits
                  .size())
          .isEqualTo(expectedResultCount);
      // TODO: Assert other fields in addition to hits.
    }

    @Test
    public void testSearchInReadOnlyChunk() {
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      int offset = 1;
      for (LogMessage m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      chunk.commit();

      assertThat(chunk.isReadOnly()).isFalse();
      chunk.setReadOnly(true);
      assertThat(chunk.isReadOnly()).isTrue();

      SearchResult<LogMessage> results =
          chunk.query(
              new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000));
      assertThat(results.hits.size()).isEqualTo(1);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testAddMessageToReadOnlyChunk() {
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      int offset = 1;
      for (LogMessage m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      chunk.commit();

      assertThat(chunk.isReadOnly()).isFalse();
      chunk.setReadOnly(true);
      assertThat(chunk.isReadOnly()).isTrue();
      chunk.addMessage(MessageUtil.makeMessage(101), TEST_KAFKA_PARTITION_ID, offset);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMessageFromDifferentPartitionFails() {
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      int offset = 1;
      for (LogMessage m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      chunk.commit();

      assertThat(chunk.isReadOnly()).isFalse();
      chunk.setReadOnly(true);
      assertThat(chunk.isReadOnly()).isTrue();
      chunk.addMessage(MessageUtil.makeMessage(101), "differentKafkaPartition", offset);
    }

    @Test
    public void testCommitBeforeSnapshot() {
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      int offset = 1;
      for (LogMessage m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      assertThat(chunk.isReadOnly()).isFalse();

      SearchResult<LogMessage> resultsBeforeCommit =
          chunk.query(
              new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000));
      assertThat(resultsBeforeCommit.hits.size()).isEqualTo(0);

      // Snapshot forces commit and refresh
      chunk.preSnapshot();
      assertThat(chunk.isReadOnly()).isTrue();
      SearchResult<LogMessage> resultsAfterPreSnapshot =
          chunk.query(
              new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000));
      assertThat(resultsAfterPreSnapshot.hits.size()).isEqualTo(1);
    }
  }

  public static class SnapshotTests {
    @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public TemporaryFolder localDownloadFolder = new TemporaryFolder();

    private SimpleMeterRegistry registry;
    private ReadWriteChunk<LogMessage> chunk;
    private TestingServer testingServer;
    private MetadataStore metadataStore;
    private boolean closeChunk;
    private SnapshotMetadataStore snapshotMetadataStore;
    private SearchMetadataStore searchMetadataStore;

    @Before
    public void setUp() throws Exception {
      Tracing.newBuilder().build();
      testingServer = new TestingServer();
      KaldbConfigs.ZookeeperConfig zkConfig =
          KaldbConfigs.ZookeeperConfig.newBuilder()
              .setZkConnectString(testingServer.getConnectString())
              .setZkPathPrefix("shouldHandleChunkLivecycle")
              .setZkSessionTimeoutMs(1000)
              .setZkConnectionTimeoutMs(1000)
              .setSleepBetweenRetriesMs(1000)
              .build();

      registry = new SimpleMeterRegistry();

      metadataStore = ZookeeperMetadataStoreImpl.fromConfig(registry, zkConfig);

      snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
      searchMetadataStore = new SearchMetadataStore(metadataStore, true);

      final LuceneIndexStoreImpl logStore =
          LuceneIndexStoreImpl.makeLogStore(
              temporaryFolder.newFolder(), COMMIT_INTERVAL, REFRESH_INTERVAL, registry);
      chunk =
          new IndexingChunkImpl<>(
              logStore,
              CHUNK_DATA_PREFIX,
              registry,
              searchMetadataStore,
              snapshotMetadataStore,
              new SearchContext(TEST_HOST, TEST_PORT),
              TEST_KAFKA_PARTITION_ID);
      chunk.postCreate();
      closeChunk = true;
      List<SnapshotMetadata> snapshotNodes =
          snapshotMetadataStore.list().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
      assertThat(snapshotNodes.size()).isEqualTo(1);
      List<SearchMetadata> searchNodes =
          searchMetadataStore.list().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
      assertThat(searchNodes.size()).isEqualTo(1);
    }

    @After
    public void tearDown() throws IOException, TimeoutException {
      if (closeChunk) chunk.close();
      searchMetadataStore.close();
      snapshotMetadataStore.close();
      metadataStore.close();
      testingServer.close();
      registry.close();
    }

    @Test
    public void testSnapshotToNonExistentS3BucketFails()
        throws ExecutionException, InterruptedException, TimeoutException {
      testBeforeSnapshotState(snapshotMetadataStore, searchMetadataStore, chunk);
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      int offset = 1;
      for (LogMessage m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }

      // Initiate pre-snapshot
      chunk.preSnapshot();

      SearchQuery searchQuery =
          new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000);
      assertThat(chunk.isReadOnly()).isTrue();
      SearchResult<LogMessage> resultsAfterPreSnapshot = chunk.query(searchQuery);
      assertThat(resultsAfterPreSnapshot.hits.size()).isEqualTo(1);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(1);
      assertThat(getCount(INDEX_FILES_UPLOAD, registry)).isEqualTo(0);
      assertThat(getCount(INDEX_FILES_UPLOAD_FAILED, registry)).isEqualTo(0);

      // create an S3 client for test
      String bucket = "invalid-bucket";
      S3Client s3Client = S3_MOCK_RULE.createS3ClientV2();
      S3BlobFs s3BlobFs = new S3BlobFs(s3Client);

      // Snapshot to S3 without creating the s3 bucket.
      assertThat(chunk.snapshotToS3(bucket, "", s3BlobFs)).isFalse();
      assertThat(chunk.info().getSnapshotPath()).isEqualTo(SnapshotMetadata.LIVE_SNAPSHOT_PATH);

      // Metadata checks
      List<SnapshotMetadata> afterSnapshots =
          snapshotMetadataStore.list().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
      assertThat(afterSnapshots.size()).isEqualTo(1);
      assertThat(afterSnapshots.get(0).partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
      assertThat(afterSnapshots.get(0).maxOffset).isEqualTo(0);
      assertThat(afterSnapshots.get(0).snapshotPath).isEqualTo(SnapshotMetadata.LIVE_SNAPSHOT_PATH);

      List<SearchMetadata> afterSearchNodes =
          searchMetadataStore.list().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
      assertThat(afterSearchNodes.size()).isEqualTo(1);
      assertThat(afterSearchNodes.get(0).url).contains(TEST_HOST);
      assertThat(afterSearchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
      assertThat(afterSearchNodes.get(0).snapshotName)
          .contains(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    }

    // TODO: Add a test to check that the data is deleted from the file system on cleanup.

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testSnapshotToS3UsingChunkApi() throws Exception {
      testBeforeSnapshotState(snapshotMetadataStore, searchMetadataStore, chunk);
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      int offset = 1;
      for (LogMessage m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }

      // Initiate pre-snapshot
      chunk.preSnapshot();

      SearchQuery searchQuery =
          new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000);
      assertThat(chunk.isReadOnly()).isTrue();
      SearchResult<LogMessage> resultsAfterPreSnapshot = chunk.query(searchQuery);
      assertThat(resultsAfterPreSnapshot.hits.size()).isEqualTo(1);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(1);
      assertThat(getCount(INDEX_FILES_UPLOAD, registry)).isEqualTo(0);
      assertThat(getCount(INDEX_FILES_UPLOAD_FAILED, registry)).isEqualTo(0);

      // create an S3 client for test
      String bucket = "test-bucket-with-prefix";
      S3Client s3Client = S3_MOCK_RULE.createS3ClientV2();
      S3BlobFs s3BlobFs = new S3BlobFs(s3Client);
      s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

      // Snapshot to S3
      assertThat(chunk.info().getSnapshotPath()).isEqualTo(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
      assertThat(chunk.snapshotToS3(bucket, "", s3BlobFs)).isTrue();
      assertThat(chunk.info().getSnapshotPath()).isNotEmpty();

      assertThat(getCount(INDEX_FILES_UPLOAD, registry)).isEqualTo(15);
      assertThat(getCount(INDEX_FILES_UPLOAD_FAILED, registry)).isEqualTo(0);
      assertThat(registry.get(SNAPSHOT_TIMER).timer().totalTime(TimeUnit.SECONDS)).isGreaterThan(0);

      // Post snapshot cleanup.
      chunk.postSnapshot();

      // Metadata checks
      List<SnapshotMetadata> afterSnapshots =
          snapshotMetadataStore.list().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
      assertThat(afterSnapshots.size()).isEqualTo(2);
      assertThat(afterSnapshots).contains(ChunkInfo.toSnapshotMetadata(chunk.info(), ""));
      SnapshotMetadata liveSnapshot =
          afterSnapshots
              .stream()
              .filter(s -> s.snapshotPath.equals(SnapshotMetadata.LIVE_SNAPSHOT_PATH))
              .findFirst()
              .get();
      assertThat(liveSnapshot.partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
      assertThat(liveSnapshot.maxOffset).isEqualTo(offset - 1);
      assertThat(liveSnapshot.snapshotPath).isEqualTo(SnapshotMetadata.LIVE_SNAPSHOT_PATH);

      List<SearchMetadata> afterSearchNodes =
          searchMetadataStore.list().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
      assertThat(afterSearchNodes.size()).isEqualTo(1);
      assertThat(afterSearchNodes.get(0).url).contains(TEST_HOST);
      assertThat(afterSearchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
      assertThat(afterSearchNodes.get(0).snapshotName)
          .contains(SnapshotMetadata.LIVE_SNAPSHOT_PATH);

      chunk.close();
      closeChunk = false;
    }
  }
}
