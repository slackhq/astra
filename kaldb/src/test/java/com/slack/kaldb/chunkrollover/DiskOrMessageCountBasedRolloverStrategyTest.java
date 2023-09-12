package com.slack.kaldb.chunkrollover;

import static com.slack.kaldb.chunkrollover.DiskOrMessageCountBasedRolloverStrategy.LIVE_BYTES_DIR;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.TEST_HOST;
import static com.slack.kaldb.testlib.ChunkManagerUtil.TEST_PORT;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.blobfs.s3.S3CrtBlobFs;
import com.slack.kaldb.blobfs.s3.S3TestUtils;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class DiskOrMessageCountBasedRolloverStrategyTest {
  private static final String S3_TEST_BUCKET = "test-kaldb-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private static final String TEST_KAFKA_PARTITION_ID = "10";
  @TempDir private Path tmpPath;

  private IndexingChunkManager<LogMessage> chunkManager = null;

  private SimpleMeterRegistry metricsRegistry;
  private S3AsyncClient s3AsyncClient;
  private static final String ZK_PATH_PREFIX = "testZK";
  private S3CrtBlobFs s3CrtBlobFs;
  private TestingServer localZkServer;
  private AsyncCuratorFramework curatorFramework;
  private SnapshotMetadataStore snapshotMetadataStore;
  private SearchMetadataStore searchMetadataStore;

  private static long MAX_BYTES_PER_CHUNK = 12000;

  private KaldbLocalQueryService<LogMessage> kaldbLocalQueryService;

  @BeforeEach
  public void setUp() throws Exception {
    DiskOrMessageCountBasedRolloverStrategy.DIRECTORY_SIZE_EXECUTOR_PERIOD_MS = 10;
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();

    s3AsyncClient = S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    s3CrtBlobFs = new S3CrtBlobFs(s3AsyncClient);

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

    curatorFramework = CuratorBuilder.build(metricsRegistry, zkConfig);
    snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    searchMetadataStore = new SearchMetadataStore(curatorFramework, false);
  }

  @AfterEach
  public void tearDown() throws TimeoutException, IOException {
    metricsRegistry.close();
    if (chunkManager != null) {
      chunkManager.stopAsync();
      chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }
    if (s3AsyncClient != null) {
      s3AsyncClient.close();
    }
    if (localZkServer != null) {
      localZkServer.stop();
    }
  }

  private void initChunkManager(
      ChunkRollOverStrategy chunkRollOverStrategy,
      String s3TestBucket,
      ListeningExecutorService listeningExecutorService)
      throws IOException, TimeoutException {
    SearchContext searchContext = new SearchContext(TEST_HOST, TEST_PORT);
    chunkManager =
        new IndexingChunkManager<>(
            "testData",
            tmpPath.toFile().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3CrtBlobFs,
            s3TestBucket,
            listeningExecutorService,
            curatorFramework,
            searchContext,
            KaldbConfigUtil.makeIndexerConfig(TEST_PORT, 1000, "log_message", 100));
    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    kaldbLocalQueryService = new KaldbLocalQueryService<>(chunkManager, Duration.ofSeconds(3));
  }

  @Test
  public void testInitViaConfig() {
    KaldbConfigs.IndexerConfig indexerCfg = KaldbConfigUtil.makeIndexerConfig();
    assertThat(indexerCfg.getMaxMessagesPerChunk()).isEqualTo(100);
    assertThat(indexerCfg.getMaxBytesPerChunk()).isEqualTo(10737418240L);
    DiskOrMessageCountBasedRolloverStrategy chunkRollOverStrategy =
        DiskOrMessageCountBasedRolloverStrategy.fromConfig(metricsRegistry, indexerCfg);
    assertThat(chunkRollOverStrategy.getMaxBytesPerChunk()).isEqualTo(10737418240L);
    chunkRollOverStrategy.close();
  }

  @Test
  @Disabled // flakey test
  public void testDiskBasedRolloverWithMaxBytes() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(
            metricsRegistry, MAX_BYTES_PER_CHUNK, 1_000_000_000);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

    int totalMessages = 10;
    List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, totalMessages, 1000, startTime);
    int offset = 1;
    boolean shouldCheckOnNextMessage = false;
    for (LogMessage m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      offset++;
      Thread.sleep(DiskOrMessageCountBasedRolloverStrategy.DIRECTORY_SIZE_EXECUTOR_PERIOD_MS);
      if (chunkManager.getActiveChunk() != null) {
        chunkManager.getActiveChunk().commit();
      }
      // this doesn't work because the next active chunk gets assigned only on next message add
      //        await()
      //            .untilAsserted(
      //                () -> assertThat(getValue(LIVE_BYTES_DIR, metricsRegistry)).isEqualTo(0));
      if (shouldCheckOnNextMessage) {
        assertThat(getValue(LIVE_BYTES_DIR, metricsRegistry)).isEqualTo(0);
      }
      shouldCheckOnNextMessage = getValue(LIVE_BYTES_DIR, metricsRegistry) > MAX_BYTES_PER_CHUNK;
    }
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (totalMessages * 1000);
    KaldbSearch.SearchResult response =
        kaldbLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQueryString("Message1")
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregations(
                    KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
                        .setType(DateHistogramAggBuilder.TYPE)
                        .setName("1")
                        .setValueSource(
                            KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                .newBuilder()
                                .setField(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)
                                .setDateHistogram(
                                    KaldbSearch.SearchRequest.SearchAggregation
                                        .ValueSourceAggregation.DateHistogramAggregation
                                        .newBuilder()
                                        .setMinDocCount(1)
                                        .setInterval("1s")
                                        .build())
                                .build())
                        .build())
                .build());

    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getTotalSnapshots()).isEqualTo(3);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(3);
  }

  @Test
  public void testNegativeMaxMessagesPerChunk() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new MessageSizeOrCountBasedRolloverStrategy(metricsRegistry, 100, -1));
  }

  @Test
  public void testNegativeMaxBytesPerChunk() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new MessageSizeOrCountBasedRolloverStrategy(metricsRegistry, -100, 1));
  }

  @Test
  public void testDiskBasedRolloverWithMaxMessages() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, Long.MAX_VALUE, 4);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

    int totalMessages = 10;
    List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, totalMessages, 1000, startTime);
    int offset = 1;
    for (LogMessage m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      offset++;
      if (chunkManager.getActiveChunk() != null) {
        chunkManager.getActiveChunk().commit();
      }
    }
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 2);

    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (totalMessages * 1000);
    KaldbSearch.SearchResult response =
        kaldbLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQueryString("Message1")
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregations(
                    KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
                        .setType(DateHistogramAggBuilder.TYPE)
                        .setName("1")
                        .setValueSource(
                            KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                .newBuilder()
                                .setField(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)
                                .setDateHistogram(
                                    KaldbSearch.SearchRequest.SearchAggregation
                                        .ValueSourceAggregation.DateHistogramAggregation
                                        .newBuilder()
                                        .setInterval("1s")
                                        .setMinDocCount(1)
                                        .build())
                                .build())
                        .build())
                .build());

    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getTotalSnapshots()).isEqualTo(3);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(3);
  }

  @Test
  public void testChunkRollOver() {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 1000, 2000);

    assertThat(chunkRollOverStrategy.shouldRollOver(1, 1)).isFalse();
    assertThat(chunkRollOverStrategy.shouldRollOver(-1, -1)).isFalse();
    assertThat(chunkRollOverStrategy.shouldRollOver(0, 0)).isFalse();
    assertThat(chunkRollOverStrategy.shouldRollOver(100, 100)).isFalse();
    assertThat(chunkRollOverStrategy.shouldRollOver(1000, 1)).isFalse();
    assertThat(chunkRollOverStrategy.shouldRollOver(1001, 1)).isFalse();

    assertThat(chunkRollOverStrategy.shouldRollOver(100, 2000)).isTrue();
    assertThat(chunkRollOverStrategy.shouldRollOver(100, 2001)).isTrue();
    assertThat(chunkRollOverStrategy.shouldRollOver(1001, 2001)).isTrue();
  }

  @Test
  public void testCalculateDirectorySize() throws IOException {
    FSDirectory directory = mock(FSDirectory.class);
    when(directory.listAll()).thenThrow(IOException.class);
    assertThat(DiskOrMessageCountBasedRolloverStrategy.calculateDirectorySize(directory))
        .isEqualTo(-1);

    directory = mock(FSDirectory.class);
    when(directory.listAll()).thenReturn(new String[] {"file1", "file2"});
    when(directory.fileLength("file1")).thenReturn(1L);
    when(directory.fileLength("file2")).thenReturn(2L);
    assertThat(DiskOrMessageCountBasedRolloverStrategy.calculateDirectorySize(directory))
        .isEqualTo(3);

    directory = mock(FSDirectory.class);
    when(directory.listAll()).thenReturn(new String[] {"file1", "file2"});
    when(directory.fileLength("file1")).thenReturn(1L);
    when(directory.fileLength("file2")).thenThrow(IOException.class);
    assertThat(DiskOrMessageCountBasedRolloverStrategy.calculateDirectorySize(directory))
        .isEqualTo(1);
  }
}
