package com.slack.astra.chunk;

import static com.slack.astra.chunk.ReadWriteChunk.INDEX_FILES_UPLOAD;
import static com.slack.astra.chunk.ReadWriteChunk.INDEX_FILES_UPLOAD_FAILED;
import static com.slack.astra.chunk.ReadWriteChunk.LIVE_SNAPSHOT_PREFIX;
import static com.slack.astra.chunk.ReadWriteChunk.SCHEMA_FILE_NAME;
import static com.slack.astra.chunk.ReadWriteChunk.SNAPSHOT_TIMER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.MetricsUtil.getTimerCount;
import static com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LuceneIndexStoreImpl;
import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.util.QueryBuilderUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

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
      ReadWriteChunk<LogMessage> chunk) {
    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsOnly(ChunkInfo.toSnapshotMetadata(chunk.info(), LIVE_SNAPSHOT_PREFIX));
    final List<SearchMetadata> beforeSearchNodes =
        AstraMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(beforeSearchNodes.size()).isEqualTo(1);
    assertThat(beforeSearchNodes.get(0).url).contains(TEST_HOST);
    assertThat(beforeSearchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
  }

  @Nested
  public class BasicTests {
    @TempDir private Path tmpPath;

    private boolean closeChunk = true;
    private MeterRegistry registry;
    private ReadWriteChunk<LogMessage> chunk;
    private TestingServer testingServer;
    private AsyncCuratorFramework curatorFramework;

    @BeforeEach
    public void setUp() throws Exception {
      Tracing.newBuilder().build();

      testingServer = new TestingServer();
      AstraConfigs.MetadataStoreConfig metadataStoreConfig =
          AstraConfigs.MetadataStoreConfig.newBuilder()
              .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
              .setZookeeperConfig(
                  AstraConfigs.ZookeeperConfig.newBuilder()
                      .setZkConnectString(testingServer.getConnectString())
                      .setZkPathPrefix("shouldHandleChunkLivecycle")
                      .setZkSessionTimeoutMs(1000)
                      .setZkConnectionTimeoutMs(1000)
                      .setSleepBetweenRetriesMs(1000)
                      .setZkCacheInitTimeoutMs(1000)
                      .build())
              .build();

      registry = new SimpleMeterRegistry();

      curatorFramework = CuratorBuilder.build(registry, metadataStoreConfig.getZookeeperConfig());

      SnapshotMetadataStore snapshotMetadataStore =
          new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, registry);
      SearchMetadataStore searchMetadataStore =
          new SearchMetadataStore(curatorFramework, metadataStoreConfig, registry, true);

      final LuceneIndexStoreImpl logStore =
          LuceneIndexStoreImpl.makeLogStore(
              tmpPath.toFile(),
              COMMIT_INTERVAL,
              REFRESH_INTERVAL,
              true,
              SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy
                  .CONVERT_VALUE_AND_DUPLICATE_FIELD,
              registry);
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

    @AfterEach
    public void tearDown() throws IOException, TimeoutException {
      if (closeChunk) chunk.close();

      curatorFramework.unwrap().close();
      testingServer.close();
      registry.close();
    }

    @Test
    public void testAddAndSearchChunk() throws IOException {
      List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now());
      int offset = 1;
      for (Trace.Span m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      chunk.commit();

      chunk.query(
          new SearchQuery(
              MessageUtil.TEST_DATASET_NAME,
              0L,
              MAX_TIME,
              10,
              Collections.emptyList(),
              QueryBuilderUtil.generateQueryBuilder("*:*", 0L, MAX_TIME),
              null,
              createGenericDateHistogramAggregatorFactoriesBuilder()));

      chunk.query(
          new SearchQuery(
              MessageUtil.TEST_DATASET_NAME,
              0L,
              MAX_TIME,
              10,
              Collections.emptyList(),
              QueryBuilderUtil.generateQueryBuilder("Message1", 0L, MAX_TIME),
              null,
              createGenericDateHistogramAggregatorFactoriesBuilder()));

      SearchResult<LogMessage> results =
          chunk.query(
              new SearchQuery(
                  MessageUtil.TEST_DATASET_NAME,
                  0L,
                  MAX_TIME,
                  10,
                  Collections.emptyList(),
                  QueryBuilderUtil.generateQueryBuilder("Message*", 0L, MAX_TIME),
                  null,
                  createGenericDateHistogramAggregatorFactoriesBuilder()));
      assertThat(results.hits.size()).isEqualTo(10);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(1);
    }

    @Test
    public void testAddAndSearchChunkInTimeRange() throws IOException {
      final Instant startTime = Instant.now();
      List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
      final long messageStartTimeMs =
          TimeUnit.MILLISECONDS.convert(messages.get(0).getTimestamp(), TimeUnit.MICROSECONDS);
      int offset = 1;
      for (Trace.Span m : messages) {
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
      Instant oneMinBefore = Instant.now().minus(1, ChronoUnit.MINUTES);
      Instant oneMinBeforeAfter = Instant.now().plus(1, ChronoUnit.MINUTES);
      assertThat(chunk.info().getDataStartTimeEpochMs()).isGreaterThan(oneMinBefore.toEpochMilli());
      assertThat(chunk.info().getDataStartTimeEpochMs())
          .isLessThan(oneMinBeforeAfter.toEpochMilli());
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
      List<Trace.Span> newMessages =
          SpanUtil.makeSpansWithTimeDifference(
              1, 100, 1000, startTime.plus(10, ChronoUnit.MINUTES));
      final long newMessageStartTimeEpochMs =
          TimeUnit.MILLISECONDS.convert(newMessages.get(0).getTimestamp(), TimeUnit.MICROSECONDS);
      final long newMessageEndTimeEpochMs =
          TimeUnit.MILLISECONDS.convert(newMessages.get(99).getTimestamp(), TimeUnit.MICROSECONDS);
      for (Trace.Span m : newMessages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      chunk.commit();

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(200);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(2);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(2);

      assertThat(chunk.info().getDataStartTimeEpochMs()).isGreaterThan(oneMinBefore.toEpochMilli());
      assertThat(chunk.info().getDataStartTimeEpochMs())
          .isLessThan(oneMinBeforeAfter.toEpochMilli());
      assertThat(chunk.info().getDataEndTimeEpochMs()).isEqualTo(newMessageEndTimeEpochMs);

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
        String searchString, long startTimeMs, long endTimeMs, int expectedResultCount)
        throws IOException {
      assertThat(
              chunk
                  .query(
                      new SearchQuery(
                          MessageUtil.TEST_DATASET_NAME,
                          startTimeMs,
                          endTimeMs,
                          10,
                          Collections.emptyList(),
                          QueryBuilderUtil.generateQueryBuilder(
                              searchString, startTimeMs, endTimeMs),
                          null,
                          createGenericDateHistogramAggregatorFactoriesBuilder()))
                  .hits
                  .size())
          .isEqualTo(expectedResultCount);
      // TODO: Assert other fields in addition to hits.
    }

    @Test
    public void testSearchInReadOnlyChunk() throws IOException {
      List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now());
      int offset = 1;
      for (Trace.Span m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      chunk.commit();

      assertThat(chunk.isReadOnly()).isFalse();
      chunk.setReadOnly(true);
      assertThat(chunk.isReadOnly()).isTrue();

      SearchResult<LogMessage> results =
          chunk.query(
              new SearchQuery(
                  MessageUtil.TEST_DATASET_NAME,
                  0L,
                  MAX_TIME,
                  10,
                  Collections.emptyList(),
                  QueryBuilderUtil.generateQueryBuilder("Message1", 0L, MAX_TIME),
                  null,
                  createGenericDateHistogramAggregatorFactoriesBuilder()));
      assertThat(results.hits.size()).isEqualTo(1);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(1);
    }

    @Test
    public void testAddMessageToReadOnlyChunk() {
      List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now());
      int offset = 1;
      for (Trace.Span m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      chunk.commit();

      assertThat(chunk.isReadOnly()).isFalse();
      chunk.setReadOnly(true);
      assertThat(chunk.isReadOnly()).isTrue();

      int finalOffset = offset;
      assertThatExceptionOfType(IllegalStateException.class)
          .isThrownBy(
              () -> chunk.addMessage(SpanUtil.makeSpan(101), TEST_KAFKA_PARTITION_ID, finalOffset));
    }

    @Test
    public void testMessageFromDifferentPartitionFails() {
      List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now());
      int offset = 1;
      for (Trace.Span m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      chunk.commit();

      assertThat(chunk.isReadOnly()).isFalse();
      chunk.setReadOnly(true);
      assertThat(chunk.isReadOnly()).isTrue();

      int finalOffset = offset;
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(
              () ->
                  chunk.addMessage(SpanUtil.makeSpan(101), "differentKafkaPartition", finalOffset));
    }

    @Test
    public void testCommitBeforeSnapshot() throws IOException {
      List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now());
      int offset = 1;
      for (Trace.Span m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }
      assertThat(chunk.isReadOnly()).isFalse();

      SearchResult<LogMessage> resultsBeforeCommit =
          chunk.query(
              new SearchQuery(
                  MessageUtil.TEST_DATASET_NAME,
                  0L,
                  MAX_TIME,
                  10,
                  Collections.emptyList(),
                  QueryBuilderUtil.generateQueryBuilder("Message1", 0L, MAX_TIME),
                  null,
                  createGenericDateHistogramAggregatorFactoriesBuilder()));
      assertThat(resultsBeforeCommit.hits.size()).isEqualTo(0);

      // Snapshot forces commit and refresh
      chunk.preSnapshot();
      assertThat(chunk.isReadOnly()).isTrue();
      SearchResult<LogMessage> resultsAfterPreSnapshot =
          chunk.query(
              new SearchQuery(
                  MessageUtil.TEST_DATASET_NAME,
                  0L,
                  MAX_TIME,
                  10,
                  Collections.emptyList(),
                  QueryBuilderUtil.generateQueryBuilder("Message1", 0L, MAX_TIME),
                  null,
                  createGenericDateHistogramAggregatorFactoriesBuilder()));
      assertThat(resultsAfterPreSnapshot.hits.size()).isEqualTo(1);
    }
  }

  @Nested
  public class TestChunkWithRaiseErrorPolicy {
    @TempDir private Path tmpPath;

    private boolean closeChunk = true;
    private MeterRegistry registry;
    private ReadWriteChunk<LogMessage> chunk;
    private TestingServer testingServer;
    private AsyncCuratorFramework curatorFramework;

    @BeforeEach
    public void setUp() throws Exception {
      Tracing.newBuilder().build();

      testingServer = new TestingServer();
      AstraConfigs.MetadataStoreConfig metadataStoreConfig =
          AstraConfigs.MetadataStoreConfig.newBuilder()
              .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
              .setZookeeperConfig(
                  AstraConfigs.ZookeeperConfig.newBuilder()
                      .setZkConnectString(testingServer.getConnectString())
                      .setZkPathPrefix("shouldHandleChunkLivecycle")
                      .setZkSessionTimeoutMs(1000)
                      .setZkConnectionTimeoutMs(1000)
                      .setSleepBetweenRetriesMs(1000)
                      .setZkCacheInitTimeoutMs(1000)
                      .build())
              .build();

      registry = new SimpleMeterRegistry();

      curatorFramework = CuratorBuilder.build(registry, metadataStoreConfig.getZookeeperConfig());

      SnapshotMetadataStore snapshotMetadataStore =
          new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, registry);
      SearchMetadataStore searchMetadataStore =
          new SearchMetadataStore(curatorFramework, metadataStoreConfig, registry, true);

      final LuceneIndexStoreImpl logStore =
          LuceneIndexStoreImpl.makeLogStore(
              tmpPath.toFile(),
              COMMIT_INTERVAL,
              REFRESH_INTERVAL,
              true,
              SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.RAISE_ERROR,
              registry);
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

    @AfterEach
    public void tearDown() throws IOException, TimeoutException {
      if (closeChunk) chunk.close();

      curatorFramework.unwrap().close();
      testingServer.close();
      registry.close();
    }

    @Test
    public void testAddInvalidMessagesToChunk() {
      Trace.Span invalidSpan = Trace.Span.newBuilder().build();

      // An Invalid message is dropped but failure counter is incremented.
      chunk.addMessage(invalidSpan, TEST_KAFKA_PARTITION_ID, 1);
      chunk.commit();

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(1);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(1);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(1);
    }
  }

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder().silent().withSecureConnection(false).build();

  @Nested
  public class SnapshotTests {

    @TempDir private Path tmpPath;

    private SimpleMeterRegistry registry;
    private ReadWriteChunk<LogMessage> chunk;
    private TestingServer testingServer;
    private AsyncCuratorFramework curatorFramework;
    private boolean closeChunk;
    private SnapshotMetadataStore snapshotMetadataStore;
    private SearchMetadataStore searchMetadataStore;

    @BeforeEach
    public void setUp() throws Exception {
      Tracing.newBuilder().build();
      testingServer = new TestingServer();
      AstraConfigs.MetadataStoreConfig metadataStoreConfig =
          AstraConfigs.MetadataStoreConfig.newBuilder()
              .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
              .setZookeeperConfig(
                  AstraConfigs.ZookeeperConfig.newBuilder()
                      .setZkConnectString(testingServer.getConnectString())
                      .setZkPathPrefix("shouldHandleChunkLivecycle")
                      .setZkSessionTimeoutMs(1000)
                      .setZkConnectionTimeoutMs(1000)
                      .setSleepBetweenRetriesMs(1000)
                      .setZkCacheInitTimeoutMs(1000)
                      .build())
              .build();

      registry = new SimpleMeterRegistry();

      curatorFramework = CuratorBuilder.build(registry, metadataStoreConfig.getZookeeperConfig());

      snapshotMetadataStore =
          new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, registry);
      searchMetadataStore =
          new SearchMetadataStore(curatorFramework, metadataStoreConfig, registry, true);

      final LuceneIndexStoreImpl logStore =
          LuceneIndexStoreImpl.makeLogStore(
              tmpPath.toFile(),
              COMMIT_INTERVAL,
              REFRESH_INTERVAL,
              true,
              SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy
                  .CONVERT_VALUE_AND_DUPLICATE_FIELD,
              registry);
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
          AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
      assertThat(snapshotNodes.size()).isEqualTo(1);
      List<SearchMetadata> searchNodes =
          AstraMetadataTestUtils.listSyncUncached(searchMetadataStore);
      assertThat(searchNodes.size()).isEqualTo(1);
    }

    @AfterEach
    public void tearDown() throws IOException, TimeoutException {
      if (closeChunk) chunk.close();
      searchMetadataStore.close();
      snapshotMetadataStore.close();
      curatorFramework.unwrap().close();
      testingServer.close();
      registry.close();
    }

    @Test
    public void testSnapshotToNonExistentS3BucketFails()
        throws ExecutionException, InterruptedException, TimeoutException, IOException {
      testBeforeSnapshotState(snapshotMetadataStore, searchMetadataStore, chunk);
      List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now());
      int offset = 1;
      for (Trace.Span m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }

      // Initiate pre-snapshot
      chunk.preSnapshot();

      SearchQuery searchQuery =
          new SearchQuery(
              MessageUtil.TEST_DATASET_NAME,
              0L,
              MAX_TIME,
              10,
              Collections.emptyList(),
              QueryBuilderUtil.generateQueryBuilder("Message1", 0L, MAX_TIME),
              null,
              createGenericDateHistogramAggregatorFactoriesBuilder());
      assertThat(chunk.isReadOnly()).isTrue();
      SearchResult<LogMessage> resultsAfterPreSnapshot = chunk.query(searchQuery);
      assertThat(resultsAfterPreSnapshot.hits.size()).isEqualTo(1);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(2);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(1);
      assertThat(getCount(INDEX_FILES_UPLOAD, registry)).isEqualTo(0);
      assertThat(getCount(INDEX_FILES_UPLOAD_FAILED, registry)).isEqualTo(0);

      // create an S3 client for test
      String bucket = "invalid-bucket";
      S3AsyncClient s3AsyncClient =
          S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
      BlobStore blobStore = new BlobStore(s3AsyncClient, bucket);

      // Snapshot to S3 without creating the s3 bucket.
      assertThat(chunk.snapshotToS3(blobStore)).isFalse();

      // Metadata checks
      List<SnapshotMetadata> afterSnapshots =
          AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
      assertThat(afterSnapshots.size()).isEqualTo(1);
      assertThat(afterSnapshots.get(0).partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
      assertThat(afterSnapshots.get(0).maxOffset).isEqualTo(0);

      List<SearchMetadata> afterSearchNodes =
          AstraMetadataTestUtils.listSyncUncached(searchMetadataStore);
      assertThat(afterSearchNodes.size()).isEqualTo(1);
      assertThat(afterSearchNodes.get(0).url).contains(TEST_HOST);
      assertThat(afterSearchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
    }

    @Test
    public void testExcessFieldsAreDroppedButSpanIsIndexed() throws IOException {
      int offset = 1;

      // Create a span with too many fields (> 2500)
      Trace.Span.Builder spanBuilder = SpanUtil.makeSpan(offset).toBuilder();
      for (int i = 0; i < 3000; i++) {
        spanBuilder.addTags(
            Trace.KeyValue.newBuilder()
                .setKey("custom.field." + i)
                .setVStr("value" + i)
                .setIndexSignal(Trace.IndexSignal.DYNAMIC_INDEX)
                .build());
      }

      for (int i = 0; i < 100; i++) {
        spanBuilder.addTags(
            Trace.KeyValue.newBuilder()
                .setKey("schema.field." + i)
                .setVStr("value" + i)
                .setIndexSignal(Trace.IndexSignal.IN_SCHEMA_INDEX)
                .build());
      }

      Trace.Span spanWithTooManyFields = spanBuilder.build();

      // Add and commit the message
      chunk.addMessage(spanWithTooManyFields, TEST_KAFKA_PARTITION_ID, offset);
      chunk.commit();

      // The message should be accepted, though some fields may be dropped
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry))
          .withFailMessage("Expected 1 message received")
          .isEqualTo(1);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry))
          .withFailMessage("Expected 0 messages failed")
          .isEqualTo(0);
      assertThat(getTimerCount(COMMITS_TIMER, registry))
          .withFailMessage("Expected 1 commit recorded")
          .isEqualTo(1);

      long dynamicFieldsInSchema =
          chunk.getSchema().keySet().stream().filter(key -> key.startsWith("custom.field")).count();

      assertThat(dynamicFieldsInSchema)
          .withFailMessage("Schema should not exceed 1500 fields but had %s", dynamicFieldsInSchema)
          .isLessThanOrEqualTo(1500);

      long schemaFieldsInSchema =
          chunk.getSchema().keySet().stream().filter(key -> key.startsWith("schema.field")).count();

      assertThat(schemaFieldsInSchema)
          .withFailMessage("Schema should not exceed 1500 fields but had %s", dynamicFieldsInSchema)
          .isLessThanOrEqualTo(100);
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testSnapshotToS3UsingChunkApi() throws Exception {
      testBeforeSnapshotState(snapshotMetadataStore, searchMetadataStore, chunk);
      List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now());
      int offset = 1;
      for (Trace.Span m : messages) {
        chunk.addMessage(m, TEST_KAFKA_PARTITION_ID, offset);
        offset++;
      }

      // Initiate pre-snapshot
      chunk.preSnapshot();

      SearchQuery searchQuery =
          new SearchQuery(
              MessageUtil.TEST_DATASET_NAME,
              0L,
              MAX_TIME,
              10,
              Collections.emptyList(),
              QueryBuilderUtil.generateQueryBuilder("Message1", 0L, MAX_TIME),
              null,
              createGenericDateHistogramAggregatorFactoriesBuilder());
      assertThat(chunk.isReadOnly()).isTrue();
      SearchResult<LogMessage> resultsAfterPreSnapshot = chunk.query(searchQuery);
      assertThat(resultsAfterPreSnapshot.hits.size()).isEqualTo(1);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, registry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, registry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, registry)).isEqualTo(2);
      assertThat(getTimerCount(COMMITS_TIMER, registry)).isEqualTo(1);
      assertThat(getCount(INDEX_FILES_UPLOAD, registry)).isEqualTo(0);
      assertThat(getCount(INDEX_FILES_UPLOAD_FAILED, registry)).isEqualTo(0);

      // create an S3 client for test
      String bucket = "test-bucket-with-prefix";
      S3AsyncClient s3AsyncClient =
          S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
      s3AsyncClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).get();
      BlobStore blobStore = new BlobStore(s3AsyncClient, bucket);

      // Snapshot to S3
      assertThat(chunk.snapshotToS3(blobStore)).isTrue();

      // depending on heap and CFS files this can be 5 or 19.
      assertThat(getCount(INDEX_FILES_UPLOAD, registry)).isGreaterThan(5);
      assertThat(getCount(INDEX_FILES_UPLOAD_FAILED, registry)).isEqualTo(0);

      assertThat(registry.get(SNAPSHOT_TIMER).timer().totalTime(TimeUnit.SECONDS)).isGreaterThan(0);

      // Check schema file exists in s3
      ListObjectsV2Response objectsResponse =
          s3AsyncClient
              .listObjectsV2(
                  ListObjectsV2Request.builder().bucket(bucket).prefix(chunk.id()).build())
              .get();
      assertThat(
              objectsResponse.contents().stream()
                  .filter(o -> o.key().contains(SCHEMA_FILE_NAME))
                  .count())
          .isEqualTo(1);

      // Post snapshot cleanup.
      chunk.postSnapshot();

      // Metadata checks
      List<SnapshotMetadata> afterSnapshots =
          AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
      assertThat(afterSnapshots.size()).isEqualTo(2);
      assertThat(afterSnapshots).contains(ChunkInfo.toSnapshotMetadata(chunk.info(), ""));
      SnapshotMetadata liveSnapshot =
          afterSnapshots.stream().filter(SnapshotMetadata::isLive).findFirst().get();
      assertThat(liveSnapshot.partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
      assertThat(liveSnapshot.maxOffset).isEqualTo(offset - 1);
      assertThat(liveSnapshot.isLive()).isTrue();

      List<SearchMetadata> afterSearchNodes =
          AstraMetadataTestUtils.listSyncUncached(searchMetadataStore);
      assertThat(afterSearchNodes.size()).isEqualTo(1);
      assertThat(afterSearchNodes.get(0).url).contains(TEST_HOST);
      assertThat(afterSearchNodes.get(0).url).contains(String.valueOf(TEST_PORT));

      // Check total size of objects uploaded was correctly tracked
      assertThat(chunk.info().getSizeInBytesOnDisk())
          .isEqualTo(objectsResponse.contents().stream().mapToLong(S3Object::size).sum());

      chunk.close();
      // Ensure folder is cleared after chunk chlose. List the number of files in folder
      // recursively, skip empty folders.
      assertThat(
              FileUtils.listFiles(tmpPath.toFile(), TrueFileFilter.TRUE, TrueFileFilter.TRUE)
                  .stream()
                  .count())
          .isZero();
      closeChunk = false;
    }
  }
}
