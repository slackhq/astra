package com.slack.kaldb.logstore;

import static com.slack.kaldb.logstore.BlobFsUtils.DELIMITER;
import static com.slack.kaldb.logstore.BlobFsUtils.copyFromS3;
import static com.slack.kaldb.logstore.BlobFsUtils.copyToLocalPath;
import static com.slack.kaldb.logstore.BlobFsUtils.copyToS3;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getTimerCount;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.addMessages;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.findAllMessages;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.blobfs.LocalBlobFs;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.blobfs.s3.S3TestUtils;
import com.slack.kaldb.logstore.LogMessage.ReservedField;
import com.slack.kaldb.logstore.search.LogIndexSearcherImpl;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.index.IndexCommit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

@SuppressWarnings("unused")
@RunWith(Enclosed.class)
public class LuceneIndexStoreImplTest {
  @BeforeClass
  public static void beforeClass() throws Exception {
    Tracing.newBuilder().build();
  }

  public static class TestsWithForgivingLogStore {
    @Rule
    public TemporaryLogStoreAndSearcherRule forgivingLogStore =
        new TemporaryLogStoreAndSearcherRule(true);

    public TestsWithForgivingLogStore() throws IOException {}

    @Test
    public void testSimpleIndexAndQuery() {
      addMessages(forgivingLogStore.logStore, 1, 100, true);
      Collection<LogMessage> results =
          findAllMessages(
              forgivingLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "Message1", 10, 1);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, forgivingLogStore.metricsRegistry))
          .isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, forgivingLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, forgivingLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, forgivingLogStore.metricsRegistry)).isEqualTo(1);
    }

    @Test
    public void testSearchAndQueryDocsWithNestedJson() throws InterruptedException {
      // TODO: Use ImmutableMap from Guava instead of Map.of which is Java 9 only?
      LogMessage msg =
          new LogMessage(
              MessageUtil.TEST_INDEX_NAME,
              "INFO",
              "1",
              Map.of(
                  ReservedField.TIMESTAMP.fieldName,
                  MessageUtil.getCurrentLogDate(),
                  ReservedField.MESSAGE.fieldName,
                  "Test message",
                  "duplicateproperty",
                  "duplicate1",
                  "nested",
                  Map.of("key1", "value1", "duplicateproperty", 2)));
      forgivingLogStore.logStore.addMessage(msg);
      forgivingLogStore.logStore.commit();
      forgivingLogStore.logStore.refresh();
      Thread.sleep(1000);

      SearchResult<LogMessage> result1 =
          forgivingLogStore.logSearcher.search(
              MessageUtil.TEST_INDEX_NAME, "key1:value1", 0, MAX_TIME, 100, 1);
      assertThat(result1.hits.size()).isEqualTo(1);

      SearchResult<LogMessage> result2 =
          forgivingLogStore.logSearcher.search(
              MessageUtil.TEST_INDEX_NAME, "duplicateproperty:duplicate1", 0, MAX_TIME, 100, 1);
      assertThat(result2.hits.size()).isEqualTo(1);

      SearchResult<LogMessage> result3 =
          forgivingLogStore.logSearcher.search(
              MessageUtil.TEST_INDEX_NAME, "duplicateproperty:2", 0, MAX_TIME, 100, 1);
      assertThat(result3.hits.size()).isEqualTo(1);
    }

    @Test
    public void testQueryReturnsMultipleHits() {
      addMessages(forgivingLogStore.logStore, 1, 100, true);
      Collection<LogMessage> results =
          findAllMessages(
              forgivingLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "identifier", 1000, 1);
      assertThat(results.size()).isEqualTo(100);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, forgivingLogStore.metricsRegistry))
          .isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, forgivingLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, forgivingLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, forgivingLogStore.metricsRegistry)).isEqualTo(1);
    }

    @Test
    public void testTimestampOrdering() {
      List<LogMessage> msgs = addMessages(forgivingLogStore.logStore, 1, 100, true);
      List<LogMessage> results =
          findAllMessages(
              forgivingLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "identifier", 1, 1);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, forgivingLogStore.metricsRegistry))
          .isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, forgivingLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, forgivingLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, forgivingLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(results.get(0).id).isEqualTo(msgs.get(msgs.size() - 1).id);
    }

    @Test
    public void testIndexDocsWithUnsupportedPropertyTypes() {
      LogMessage msg = MessageUtil.makeMessage(100);
      MessageUtil.addFieldToMessage(msg, "unsupportedProperty", Collections.emptyList());
      forgivingLogStore.logStore.addMessage(msg);
      addMessages(forgivingLogStore.logStore, 1, 99, true);
      Collection<LogMessage> results =
          findAllMessages(
              forgivingLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "identifier", 1000, 1);
      assertThat(results.size()).isEqualTo(100);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, forgivingLogStore.metricsRegistry))
          .isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, forgivingLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, forgivingLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, forgivingLogStore.metricsRegistry)).isEqualTo(1);
    }

    @Test
    public void testIndexDocsWithTypeMismatchErrors() {
      LogMessage msg = MessageUtil.makeMessage(100);
      MessageUtil.addFieldToMessage(msg, ReservedField.HOSTNAME.fieldName, 1);
      forgivingLogStore.logStore.addMessage(msg);
      addMessages(forgivingLogStore.logStore, 1, 99, true);
      Collection<LogMessage> results =
          findAllMessages(
              forgivingLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "identifier", 1000, 1);
      assertThat(results.size()).isEqualTo(100);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, forgivingLogStore.metricsRegistry))
          .isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, forgivingLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, forgivingLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, forgivingLogStore.metricsRegistry)).isEqualTo(1);
    }
  }

  public static class TestsWithStrictLogStore {
    @Rule
    public TemporaryLogStoreAndSearcherRule strictLogStore =
        new TemporaryLogStoreAndSearcherRule(false);

    public TestsWithStrictLogStore() throws IOException {}

    @Test
    public void failIndexingDocsWithPropertyTypeErrors() {
      LogMessage msg = MessageUtil.makeMessage(100);
      MessageUtil.addFieldToMessage(msg, "unsupportedProperty", Collections.emptyList());
      strictLogStore.logStore.addMessage(msg);
      addMessages(strictLogStore.logStore, 1, 99, true);
      Collection<LogMessage> results =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "identifier", 1000, 1);
      assertThat(results.size()).isEqualTo(99);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry))
          .isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    }

    @Test
    public void failIndexingDocsWithMismatchedTypeErrors() {
      LogMessage msg = MessageUtil.makeMessage(100);
      MessageUtil.addFieldToMessage(msg, ReservedField.HOSTNAME.fieldName, 20000);
      strictLogStore.logStore.addMessage(msg);
      addMessages(strictLogStore.logStore, 1, 99, true);
      Collection<LogMessage> results =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "identifier", 1000, 1);
      assertThat(results.size()).isEqualTo(99);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry))
          .isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    }

    @Test
    public void indexLongUnbreakableField() {
      LogMessage msg = MessageUtil.makeMessage(1);
      String hugeField =
          IntStream.range(1, 10000).boxed().map(String::valueOf).collect(Collectors.joining(""));
      MessageUtil.addFieldToMessage(msg, "hugefield", hugeField);
      strictLogStore.logStore.addMessage(msg);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
      // Counters not set since no commit.
      assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(0);
    }

    @Test
    public void testFieldSearch() throws InterruptedException {
      LogMessage msg =
          new LogMessage(
              MessageUtil.TEST_INDEX_NAME,
              "INFO",
              "1",
              Map.of(
                  ReservedField.TIMESTAMP.fieldName,
                  MessageUtil.getCurrentLogDate(),
                  ReservedField.MESSAGE.fieldName,
                  "Test message",
                  ReservedField.TAG.fieldName,
                  "foo-bar",
                  ReservedField.HOSTNAME.fieldName,
                  "host1-dc2.abc.com"));
      strictLogStore.logStore.addMessage(msg);
      strictLogStore.logStore.commit();
      strictLogStore.logStore.refresh();
      Thread.sleep(1000);

      Collection<LogMessage> results =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "tag:foo", 1000, 1);
      assertThat(results.size()).isEqualTo(1);

      Collection<LogMessage> results2 =
          findAllMessages(
              strictLogStore.logSearcher,
              MessageUtil.TEST_INDEX_NAME,
              "hostname:host1-dc2.abc.com",
              1000,
              1);
      assertThat(results2.size()).isEqualTo(1);

      Collection<LogMessage> results3 =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "hostname:xyz", 1000, 1);
      assertThat(results3.size()).isEqualTo(0);

      Collection<LogMessage> results4 =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "hostname:host2", 1000, 1);
      assertThat(results4.size()).isEqualTo(0);

      Collection<LogMessage> results5 =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "hostname:abc", 1000, 1);
      assertThat(results5.size()).isEqualTo(0);

      Collection<LogMessage> results6 =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "hostname:abc.com", 1000, 1);
      assertThat(results6.size()).isEqualTo(1);

      Collection<LogMessage> results7 =
          findAllMessages(
              strictLogStore.logSearcher,
              MessageUtil.TEST_INDEX_NAME,
              "hostname:host1-dc2",
              1000,
              1);
      assertThat(results7.size()).isEqualTo(1);

      Collection<LogMessage> results8 =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "hostname:com.abc", 1000, 1);
      assertThat(results8.size()).isEqualTo(0);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    }
  }

  public static class SuppressExceptionsOnClosedWriter {
    @Rule
    public TemporaryLogStoreAndSearcherRule testLogStore =
        new TemporaryLogStoreAndSearcherRule(true);

    public SuppressExceptionsOnClosedWriter() throws IOException {}

    @Test
    public void testSearcherOnclosedWriter() {
      addMessages(testLogStore.logStore, 1, 100, true);
      testLogStore.logStore.close();
      testLogStore.logStore = null;
      Collection<LogMessage> results =
          findAllMessages(
              testLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "Message1", 100, 1);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, testLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, testLogStore.metricsRegistry)).isEqualTo(1);
    }
  }

  public static class SnapshotTester {
    @Rule
    public TemporaryLogStoreAndSearcherRule strictLogStore =
        new TemporaryLogStoreAndSearcherRule(false);

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

    public SnapshotTester() throws IOException {}

    @Test
    public void testS3SnapshotWithPrefix() throws Exception {
      testS3Snapshot("test-bucket-with-prefix", "snapshot_prefix1");
    }

    @Test
    public void testS3SnapshotWithEmptyPrefix() throws Exception {
      testS3Snapshot("test-bucket-no-prefix", "");
    }

    private void testS3Snapshot(String bucket, String prefix) throws Exception {
      LuceneIndexStoreImpl logStore = strictLogStore.logStore;
      addMessages(logStore, 1, 100, true);
      Collection<LogMessage> results =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "Message1", 100, 1);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry))
          .isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

      Path dirPath = logStore.getDirectory().toAbsolutePath();
      IndexCommit indexCommit = logStore.getIndexCommit();
      Collection<String> activeFiles = indexCommit.getFileNames();
      LocalBlobFs localBlobFs = new LocalBlobFs();

      logStore.close();
      strictLogStore.logSearcher.close();
      strictLogStore.logStore = null;
      strictLogStore.logSearcher = null;
      assertThat(localBlobFs.listFiles(dirPath.toUri(), false).length)
          .isGreaterThanOrEqualTo(activeFiles.size());

      // create an S3 client
      S3Client s3Client = S3_MOCK_RULE.createS3ClientV2();
      S3BlobFs s3BlobFs = new S3BlobFs(s3Client);
      s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

      // Copy files to S3.
      copyToS3(dirPath, activeFiles, bucket, prefix, s3BlobFs);

      for (String fileName : activeFiles) {
        File fileToCopy = new File(dirPath.toString(), fileName);
        HeadObjectResponse headObjectResponse =
            s3Client.headObject(
                S3TestUtils.getHeadObjectRequest(
                    bucket,
                    prefix != null && !prefix.isEmpty()
                        ? prefix + DELIMITER + fileName
                        : fileName));
        assertThat(headObjectResponse.contentLength()).isEqualTo(fileToCopy.length());
      }

      // Download files from S3 to local FS.
      String[] s3Files =
          copyFromS3(bucket, prefix, s3BlobFs, Paths.get(tempFolder.getRoot().getAbsolutePath()));
      assertThat(s3Files.length).isEqualTo(activeFiles.size());

      // Search files in local FS.
      LogIndexSearcherImpl newSearcher =
          new LogIndexSearcherImpl(
              LogIndexSearcherImpl.searcherManagerFromPath(tempFolder.getRoot().toPath()));
      Collection<LogMessage> newResults =
          findAllMessages(newSearcher, MessageUtil.TEST_INDEX_NAME, "Message1", 100, 1);
      assertThat(newResults.size()).isEqualTo(1);

      // Clean up
      logStore.releaseIndexCommit(indexCommit);
      newSearcher.close();
      s3BlobFs.close();
    }

    @Test
    public void testLocalSnapshot() throws IOException {
      LuceneIndexStoreImpl logStore = strictLogStore.logStore;
      addMessages(logStore, 1, 100, true);
      Collection<LogMessage> results =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "Message1", 100, 1);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry))
          .isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

      Path dirPath = logStore.getDirectory().toAbsolutePath();
      IndexCommit indexCommit = logStore.getIndexCommit();
      Collection<String> activeFiles = indexCommit.getFileNames();
      LocalBlobFs blobFs = new LocalBlobFs();
      logStore.close();
      strictLogStore.logSearcher.close();
      strictLogStore.logStore = null;
      strictLogStore.logSearcher = null;

      assertThat(blobFs.listFiles(dirPath.toUri(), false).length)
          .isGreaterThanOrEqualTo(activeFiles.size());

      copyToLocalPath(
          dirPath, activeFiles, Paths.get(tempFolder.getRoot().getAbsolutePath()), blobFs);

      LogIndexSearcherImpl newSearcher =
          new LogIndexSearcherImpl(
              LogIndexSearcherImpl.searcherManagerFromPath(tempFolder.getRoot().toPath()));

      Collection<LogMessage> newResults =
          findAllMessages(newSearcher, MessageUtil.TEST_INDEX_NAME, "Message1", 100, 1);
      assertThat(newResults.size()).isEqualTo(1);
      logStore.releaseIndexCommit(indexCommit);
      newSearcher.close();
    }
  }

  public static class IndexCleanupTests {
    @Rule
    public TemporaryLogStoreAndSearcherRule strictLogStore =
        new TemporaryLogStoreAndSearcherRule(false);

    public IndexCleanupTests() throws IOException {}

    @Test
    public void testCleanup() throws IOException {
      addMessages(strictLogStore.logStore, 1, 100, true);
      Collection<LogMessage> results =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "Message1", 100, 1);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry))
          .isEqualTo(100);

      strictLogStore.logStore.close();
      strictLogStore.logSearcher.close();

      File tempFolder = strictLogStore.logStore.getDirectory().toFile();
      assertThat(tempFolder.exists()).isTrue();
      strictLogStore.logStore.cleanup();
      assertThat(tempFolder.exists()).isFalse();
      // Set the values to null so we don't do double cleanup.
      strictLogStore.logStore = null;
      strictLogStore.logSearcher = null;
    }
  }

  public static class AutoCommitTests {
    Duration commitDuration = Duration.ofSeconds(5);

    @Rule
    public TemporaryLogStoreAndSearcherRule testLogStore =
        new TemporaryLogStoreAndSearcherRule(commitDuration, commitDuration, true);

    public AutoCommitTests() throws IOException {}

    // NOTE: This test is very timing dependent. So, it can be flaky in CI.
    @Test
    public void testCommit() throws InterruptedException {
      addMessages(testLogStore.logStore, 1, 100, false);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, testLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(COMMITS_TIMER, testLogStore.metricsRegistry)).isEqualTo(0);

      Thread.sleep(2 * commitDuration.toMillis());
      Collection<LogMessage> results =
          findAllMessages(testLogStore.logSearcher, MessageUtil.TEST_INDEX_NAME, "Message1", 10, 1);
      assertThat(results.size()).isEqualTo(1);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, testLogStore.metricsRegistry))
          .isGreaterThanOrEqualTo(1)
          .isLessThanOrEqualTo(3);
      assertThat(getTimerCount(COMMITS_TIMER, testLogStore.metricsRegistry))
          .isGreaterThanOrEqualTo(1)
          .isLessThanOrEqualTo(3);
    }
  }
}
