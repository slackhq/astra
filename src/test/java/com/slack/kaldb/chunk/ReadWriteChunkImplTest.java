package com.slack.kaldb.chunk;

import static com.slack.kaldb.chunk.ReadWriteChunkImpl.INDEX_FILES_UPLOAD;
import static com.slack.kaldb.chunk.ReadWriteChunkImpl.INDEX_FILES_UPLOAD_FAILED;
import static com.slack.kaldb.logstore.BlobFsUtils.copyFromS3;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_COUNTER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.commons.io.FileUtils;
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
public class ReadWriteChunkImplTest {

  public static class BasicTests {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final String chunkDataPrefix = "testDataSet";

    private MeterRegistry registry;
    private final Duration commitInterval = Duration.ofSeconds(5 * 60);
    private final Duration refreshInterval = Duration.ofSeconds(5 * 60);
    private Chunk<LogMessage> chunk;

    @Before
    public void setUp() throws IOException {
      registry = new SimpleMeterRegistry();
      final LuceneIndexStoreImpl logStore =
          LuceneIndexStoreImpl.makeLogStore(
              temporaryFolder.newFolder(), commitInterval, refreshInterval, registry);
      chunk = new ReadWriteChunkImpl<>(logStore, chunkDataPrefix, registry);
    }

    @After
    public void tearDown() {
      chunk.close();
      registry.close();
    }

    @Test
    public void testAddAndSearchChunk() {
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      for (LogMessage m : messages) {
        chunk.addMessage(m);
      }
      chunk.commit();

      SearchResult<LogMessage> results =
          chunk.query(
              new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000));
      assertThat(results.hits.size()).isEqualTo(1);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getCount(REFRESHES_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(COMMITS_COUNTER, registry)).isEqualTo(1);
    }

    @Test
    public void testAddInvalidMessagesToChunk() {
      LogMessage testMessage = MessageUtil.makeMessage(0);
      testMessage.addProperty("username", 0);

      // An Invalid message is dropped but failure counter is incremented.
      chunk.addMessage(testMessage);
      chunk.commit();

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(REFRESHES_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(COMMITS_COUNTER, registry)).isEqualTo(1);
    }

    @Test
    public void testAddAndSearchChunkInTimeRange() {
      final LocalDateTime startTime = LocalDateTime.of(2020, 10, 1, 10, 10, 0);
      final List<LogMessage> messages =
          MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
      final long messageStartTime = messages.get(0).timeSinceEpochMilli;
      for (LogMessage m : messages) {
        chunk.addMessage(m);
      }
      chunk.commit();

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getCount(REFRESHES_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(COMMITS_COUNTER, registry)).isEqualTo(1);

      final long expectedStartTimeEpochSecs = messageStartTime / 1000;
      final long expectedEndTimeEpochSecs = expectedStartTimeEpochSecs + 99;
      // Ensure chunk info is correct.
      assertThat(chunk.info().getDataStartTimeEpochSecs()).isEqualTo(expectedStartTimeEpochSecs);
      assertThat(chunk.info().getDataEndTimeEpochSecs()).isEqualTo(expectedEndTimeEpochSecs);
      assertThat(chunk.info().chunkId).contains(chunkDataPrefix);
      assertThat(chunk.info().getChunkSnapshotTimeEpochSecs()).isZero();
      assertThat(chunk.info().getChunkCreationTimeEpochSecs()).isPositive();

      // Search for message in expected time range.
      searchChunk(
          "Message1", expectedStartTimeEpochSecs * 1000, expectedEndTimeEpochSecs * 1000, 1);

      // Search for message before and after the time range.
      searchChunk("Message1", 0, (expectedStartTimeEpochSecs - 1) * 1000, 0);
      searchChunk("Message1", (expectedEndTimeEpochSecs + 1) * 1000, MAX_TIME, 0);

      // Search for Message1 in time range.
      searchChunk("Message1", 0, expectedStartTimeEpochSecs * 1000, 1);
      searchChunk("Message100", 0, expectedStartTimeEpochSecs * 1000, 0);

      // Search for Message100 in time range.
      searchChunk(
          "Message100", expectedStartTimeEpochSecs * 1000, expectedEndTimeEpochSecs * 1000, 1);

      // Message100 is in chunk but not in time range.
      searchChunk(
          "Message100",
          expectedStartTimeEpochSecs * 1000,
          (expectedStartTimeEpochSecs + 1) * 1000,
          0);

      // Add more messages in other time range and search again with new time ranges.
      final List<LogMessage> newMessages =
          MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime.plusDays(2));
      final long newMessageStartTimeEpochSecs = newMessages.get(0).timeSinceEpochMilli / 1000;
      for (LogMessage m : newMessages) {
        chunk.addMessage(m);
      }
      chunk.commit();

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(200);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getCount(REFRESHES_COUNTER, registry)).isEqualTo(2);
      assertThat(getCount(COMMITS_COUNTER, registry)).isEqualTo(2);

      assertThat(chunk.info().getDataStartTimeEpochSecs()).isEqualTo(expectedStartTimeEpochSecs);
      assertThat(chunk.info().getDataEndTimeEpochSecs())
          .isEqualTo(newMessageStartTimeEpochSecs + 99);

      // Search for message in expected time range.
      searchChunk(
          "Message1", expectedStartTimeEpochSecs * 1000, expectedEndTimeEpochSecs * 1000, 1);

      // Search for message before and after the time range.
      searchChunk("Message1", 0, (expectedStartTimeEpochSecs - 1) * 1000, 0);

      // Search for Message1 in time range.
      searchChunk("Message1", 0, expectedStartTimeEpochSecs * 1000, 1);

      // Search for Message100 in time range.
      searchChunk(
          "Message100", expectedStartTimeEpochSecs * 1000, expectedEndTimeEpochSecs * 1000, 1);

      // Message100 is in chunk but not in time range.
      searchChunk(
          "Message100",
          expectedStartTimeEpochSecs * 1000,
          (expectedStartTimeEpochSecs + 1) * 1000,
          0);

      // Search for new and old messages
      searchChunk("Message1", (expectedStartTimeEpochSecs + 1) * 1000, MAX_TIME, 1);
      searchChunk(
          "Message1",
          expectedStartTimeEpochSecs * 1000,
          (newMessageStartTimeEpochSecs + 100) * 1000,
          2);
      searchChunk("Message1", expectedStartTimeEpochSecs * 1000, MAX_TIME, 2);

      // Search for Message100 in time range.
      searchChunk(
          "Message100",
          expectedStartTimeEpochSecs * 1000,
          (newMessageStartTimeEpochSecs + 100) * 1000,
          2);

      // Message100 is in chunk but not in time range.
      searchChunk("Message100", (newMessageStartTimeEpochSecs + 100) * 1000, MAX_TIME, 0);
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
      for (LogMessage m : messages) {
        chunk.addMessage(m);
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
      assertThat(getCount(REFRESHES_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(COMMITS_COUNTER, registry)).isEqualTo(1);
    }

    @Test(expected = ReadOnlyChunkInsertionException.class)
    public void testAddMessageToReadOnlyChunk() {
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      for (LogMessage m : messages) {
        chunk.addMessage(m);
      }
      chunk.commit();

      assertThat(chunk.isReadOnly()).isFalse();
      chunk.setReadOnly(true);
      assertThat(chunk.isReadOnly()).isTrue();
      chunk.addMessage(MessageUtil.makeMessage(101));
    }

    @Test(expected = IllegalStateException.class)
    public void testCleanupOnOpenChunk() {
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      for (LogMessage m : messages) {
        chunk.addMessage(m);
      }
      assertThat(chunk.isReadOnly()).isFalse();
      chunk.commit();

      SearchResult<LogMessage> results =
          chunk.query(
              new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000));
      assertThat(results.hits.size()).isEqualTo(1);

      chunk.cleanup();
    }

    @Test
    public void testCommitBeforeSnapshot() {
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      for (LogMessage m : messages) {
        chunk.addMessage(m);
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
    private final Duration commitInterval = Duration.ofSeconds(5 * 60);
    private final Duration refreshInterval = Duration.ofSeconds(5 * 60);
    private Chunk<LogMessage> chunk;

    @Before
    public void setUp() throws IOException {
      registry = new SimpleMeterRegistry();
      LuceneIndexStoreImpl logStore =
          LuceneIndexStoreImpl.makeLogStore(
              temporaryFolder.newFolder(), commitInterval, refreshInterval, registry);
      chunk = new ReadWriteChunkImpl<>(logStore, "testDataSet", registry);
    }

    @After
    public void tearDown() {
      registry.close();
    }

    @Test
    public void testSnapshotToNonExistentS3BucketFails() {
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      for (LogMessage m : messages) {
        chunk.addMessage(m);
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
      assertThat(getCount(REFRESHES_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(COMMITS_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(INDEX_FILES_UPLOAD, registry)).isEqualTo(0);
      assertThat(getCount(INDEX_FILES_UPLOAD_FAILED, registry)).isEqualTo(0);

      // create an S3 client for test
      String bucket = "invalid-bucket";
      S3Client s3Client = S3_MOCK_RULE.createS3ClientV2();
      S3BlobFs s3BlobFs = new S3BlobFs();
      s3BlobFs.init(s3Client);

      // Snapshot to S3 without creating the s3 bucket.
      assertThat(chunk.snapshotToS3(bucket, "", s3BlobFs)).isFalse();
    }

    // TODO: Add a test to check that the data is deleted from the file system on cleanup.

    @Test
    public void testSnapshotToS3UsingChunkApi() throws Exception {
      List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
      for (LogMessage m : messages) {
        chunk.addMessage(m);
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
      assertThat(getCount(REFRESHES_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(COMMITS_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(INDEX_FILES_UPLOAD, registry)).isEqualTo(0);
      assertThat(getCount(INDEX_FILES_UPLOAD_FAILED, registry)).isEqualTo(0);

      // create an S3 client for test
      String bucket = "test-bucket-with-prefix";
      S3Client s3Client = S3_MOCK_RULE.createS3ClientV2();
      S3BlobFs s3BlobFs = new S3BlobFs();
      s3BlobFs.init(s3Client);
      s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

      // Snapshot to S3
      assertThat(chunk.snapshotToS3(bucket, "", s3BlobFs)).isTrue();

      assertThat(getCount(INDEX_FILES_UPLOAD, registry)).isEqualTo(15);
      assertThat(getCount(INDEX_FILES_UPLOAD_FAILED, registry)).isEqualTo(0);

      // Post snapshot cleanup.
      chunk.postSnapshot();
      chunk.close();
      chunk.cleanup();

      // Download data from S3, create a new chunk and query the data in that chunk.
      // Download files from S3 to local FS.
      File newLocalFolderPath = localDownloadFolder.newFolder();
      String[] s3Files =
          copyFromS3(bucket, "", s3BlobFs, Paths.get(newLocalFolderPath.getAbsolutePath()));
      assertThat(FileUtils.listFiles(newLocalFolderPath, null, true).size())
          .isEqualTo(s3Files.length);

      ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
          new ReadOnlyChunkImpl<>(
              newLocalFolderPath.getAbsoluteFile().toPath(),
              new ChunkInfo("testDataSet2", 0),
              registry);
      SearchResult<LogMessage> newChunkResults = readOnlyChunk.query(searchQuery);
      assertThat(newChunkResults.hits.size()).isEqualTo(1);
      readOnlyChunk.close();
      assertThat(FileUtils.listFiles(newLocalFolderPath, null, true).size())
          .isEqualTo(s3Files.length);

      // TODO: Test search via read write chunk.query API. Also, add a few more messages to search.
      //      LuceneIndexStoreImpl rwLogStore =
      //          ReadWriteChunkImpl.makeLogStore(
      //              newLocalFolderPath, commitInterval, refreshInterval, registry);
      //      ReadWriteChunkImpl<LogMessage> readWriteChunk =
      //          new ReadWriteChunkImpl<>(rwLogStore, "testDataSet2");
      //      assertThat(FileUtils.listFiles(newLocalFolderPath, null, true).size())
      //          .isGreaterThanOrEqualTo(s3Files.length); // More files like lock files may be
      // present
      //
      //      SearchResult<LogMessage> rwChunkResults = readWriteChunk.query(searchQuery);
      //      // NOTE: The search query returns 0 results even when the data exists. Not sure why.
      //      assertThat(rwChunkResults.hits.size()).isEqualTo(1);
    }
  }
}
