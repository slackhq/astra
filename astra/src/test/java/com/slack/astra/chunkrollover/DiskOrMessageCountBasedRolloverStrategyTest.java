package com.slack.astra.chunkrollover;

import static com.slack.astra.chunkrollover.DiskOrMessageCountBasedRolloverStrategy.LIVE_BYTES_DIR;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.ChunkManagerUtil.TEST_HOST;
import static com.slack.astra.testlib.ChunkManagerUtil.TEST_PORT;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.MetricsUtil.getValue;
import static com.slack.astra.util.AggregatorJSONUtil.createGenericDateHistogramJSONBlob;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.chunk.SearchContext;
import com.slack.astra.chunkManager.IndexingChunkManager;
import com.slack.astra.chunkManager.RollOverChunkTask;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.AstraLocalQueryService;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.testlib.AstraConfigUtil;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
  private static final String S3_TEST_BUCKET = "test-astra-logs";

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
  private BlobStore blobStore;
  private TestingServer localZkServer;
  private AsyncCuratorFramework curatorFramework;
  private AstraConfigs.MetadataStoreConfig metadataStoreConfig;

  private static long MAX_BYTES_PER_CHUNK = 12000;

  private AstraLocalQueryService<LogMessage> astraLocalQueryService;

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension strictLogStore =
      new TemporaryLogStoreAndSearcherExtension(true);

  public DiskOrMessageCountBasedRolloverStrategyTest() throws IOException {}

  @BeforeEach
  public void setUp() throws Exception {
    DiskOrMessageCountBasedRolloverStrategy.DIRECTORY_SIZE_EXECUTOR_PERIOD_MS = 10;
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();

    s3AsyncClient = S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    blobStore = new BlobStore(s3AsyncClient, S3_TEST_BUCKET);

    localZkServer = new TestingServer();
    localZkServer.start();

    metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(localZkServer.getConnectString())
                    .setZkPathPrefix(ZK_PATH_PREFIX)
                    .setZkSessionTimeoutMs(15000)
                    .setZkConnectionTimeoutMs(1500)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    curatorFramework =
        CuratorBuilder.build(metricsRegistry, metadataStoreConfig.getZookeeperConfig());
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
      ListeningExecutorService listeningExecutorService)
      throws IOException, TimeoutException {
    SearchContext searchContext = new SearchContext(TEST_HOST, TEST_PORT);
    chunkManager =
        new IndexingChunkManager<>(
            "testData",
            tmpPath.toFile().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            blobStore,
            listeningExecutorService,
            curatorFramework,
            searchContext,
            AstraConfigUtil.makeIndexerConfig(TEST_PORT, 1000, 100),
            metadataStoreConfig);
    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    astraLocalQueryService = new AstraLocalQueryService<>(chunkManager, Duration.ofSeconds(3));
  }

  private static String buildQueryFromQueryString(
      String queryString, Long startTime, Long endTime) {
    return "{\"bool\":{\"filter\":[{\"range\":{\"_timesinceepoch\":{\"gte\":%d,\"lte\":%d,\"format\":\"epoch_millis\"}}},{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"%s\"}}]}}"
        .formatted(startTime, endTime, queryString);
  }

  @Test
  public void testInitViaConfig() {
    AstraConfigs.IndexerConfig indexerCfg = AstraConfigUtil.makeIndexerConfig();
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

    initChunkManager(chunkRollOverStrategy, MoreExecutors.newDirectExecutorService());

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

    int totalMessages = 10;
    int offset = 1;
    boolean shouldCheckOnNextMessage = false;
    for (Trace.Span m : SpanUtil.makeSpansWithTimeDifference(1, totalMessages, 1000, startTime)) {
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

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (totalMessages * 1000);
    AstraSearch.SearchResult response =
        astraLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(buildQueryFromQueryString("Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregationJson(
                    createGenericDateHistogramJSONBlob(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s", 1))
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
  public void testRolloverBasedOnMaxTime() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(
            metricsRegistry, Long.MAX_VALUE, Long.MAX_VALUE, 2);

    initChunkManager(chunkRollOverStrategy, MoreExecutors.newDirectExecutorService());

    // add 1 message so that new chunk is created
    // wait for 2+ seconds so that the chunk rollover code will get triggered
    // add 2nd message to trigger chunk rollover
    // add 3rd message to create new chunk
    chunkManager.addMessage(SpanUtil.makeSpan(1), 100, TEST_KAFKA_PARTITION_ID, 1);
    // so that the chunk rollover code will get triggered
    Thread.sleep(2_000 + DiskOrMessageCountBasedRolloverStrategy.DIRECTORY_SIZE_EXECUTOR_PERIOD_MS);
    chunkManager.addMessage(SpanUtil.makeSpan(2), 100, TEST_KAFKA_PARTITION_ID, 1);
    chunkManager.addMessage(SpanUtil.makeSpan(3), 100, TEST_KAFKA_PARTITION_ID, 1);

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);

    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
  }

  @Test
  public void testDiskBasedRolloverWithMaxMessages() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, Long.MAX_VALUE, 4);

    initChunkManager(chunkRollOverStrategy, MoreExecutors.newDirectExecutorService());

    final Instant startTime = Instant.now();

    int totalMessages = 10;
    int offset = 1;
    for (Trace.Span m : SpanUtil.makeSpansWithTimeDifference(1, totalMessages, 1000, startTime)) {
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

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (totalMessages * 1000);
    AstraSearch.SearchResult response =
        astraLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(buildQueryFromQueryString("Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregationJson(
                    createGenericDateHistogramJSONBlob(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s", 1))
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
  public void testDirectorySizeWithNoValidSegments() throws IOException {
    try (FSDirectory fsDirectory = FSDirectory.open(Files.createTempDirectory(null))) {
      long directorySize =
          DiskOrMessageCountBasedRolloverStrategy.calculateDirectorySize(fsDirectory);
      assertThat(directorySize).isEqualTo(0);
    }
  }

  @Test
  public void testDirectorySizeWithValidSegments() {
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1));
    strictLogStore.logStore.commit();
    FSDirectory directory = strictLogStore.logStore.getDirectory();
    long directorySize = DiskOrMessageCountBasedRolloverStrategy.calculateDirectorySize(directory);
    assertThat(directorySize).isGreaterThan(0);
  }
}
