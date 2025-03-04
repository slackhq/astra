package com.slack.astra.logstore.opensearch;

import static com.slack.astra.chunkManager.IndexingChunkManager.LIVE_BYTES_INDEXED;
import static com.slack.astra.chunkManager.IndexingChunkManager.LIVE_MESSAGES_INDEXED;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.ChunkManagerUtil.TEST_HOST;
import static com.slack.astra.testlib.ChunkManagerUtil.TEST_PORT;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.MetricsUtil.getValue;
import static com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static com.slack.astra.util.AggregatorFactoriesUtil.createTermsAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.chunk.SearchContext;
import com.slack.astra.chunkManager.IndexingChunkManager;
import com.slack.astra.chunkrollover.ChunkRollOverStrategy;
import com.slack.astra.chunkrollover.DiskOrMessageCountBasedRolloverStrategy;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.AstraConfigUtil;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.util.QueryBuilderUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;

public class ObjectMapperTest {

  private static final String TEST_KAFKA_PARTITION_ID = "10";
  @TempDir private Path tmpPath;

  private IndexingChunkManager<LogMessage> chunkManager = null;

  private SimpleMeterRegistry metricsRegistry;

  private static final String ZK_PATH_PREFIX = "testZK";
  private BlobStore blobStore;
  private TestingServer localZkServer;
  private AsyncCuratorFramework curatorFramework;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();

    localZkServer = new TestingServer();
    localZkServer.start();

    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(localZkServer.getConnectString())
            .setZkPathPrefix(ZK_PATH_PREFIX)
            .setZkSessionTimeoutMs(15000)
            .setZkConnectionTimeoutMs(1500)
            .setSleepBetweenRetriesMs(1000)
            .build();

    curatorFramework = CuratorBuilder.build(metricsRegistry, zkConfig);
  }

  @AfterEach
  public void tearDown() throws TimeoutException, IOException {
    metricsRegistry.close();
    if (chunkManager != null) {
      chunkManager.stopAsync();
      chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    curatorFramework.unwrap().close();
    localZkServer.stop();
  }

  private void initChunkManager(
      ChunkRollOverStrategy chunkRollOverStrategy,
      BlobStore blobStore,
      ListeningExecutorService listeningExecutorService)
      throws IOException, TimeoutException {
    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
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
  }

  private SearchQuery newTermsAggregatorCountSearchQuery(String name, String fieldName)
      throws IOException {
    return new SearchQuery(
        MessageUtil.TEST_DATASET_NAME,
        0,
        MAX_TIME,
        10,
        Collections.emptyList(),
        QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
        null,
        createTermsAggregatorFactoriesBuilder(
            name, List.of(), fieldName, null, 1, 1, Map.of("_count", "asc")));
  }

  // Error doing map update errorMsg=can't merge a non object mapping [alerts] with an object
  // mapping
  @Test
  public void testAddMessageChildBeforeParent() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(
            metricsRegistry, 10 * 1024 * 1024 * 1024L, 1000000L);

    initChunkManager(chunkRollOverStrategy, blobStore, MoreExecutors.newDirectExecutorService());

    // child span encountered before the parent.
    List<Trace.Span> messages =
        Arrays.asList(
            SpanUtil.makeSpansCustomKeywordTags("alerts.count", "1", 1),
            SpanUtil.makeSpansCustomKeywordTags("alerts", "1", 2));

    int actualChunkSize = 0;
    int offset = 1;
    for (Trace.Span m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      actualChunkSize += msgSize;
      offset++;

      chunkManager.getActiveChunk().commit();
    }

    // error will be thrown, but ingestion works fine.
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(2);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(actualChunkSize);

    // aggregation on alerts.count (child) works but fails for alerts (parents)

    SearchQuery searchQuery = newTermsAggregatorCountSearchQuery("test1", "alerts.count");
    SearchResult<LogMessage> results = chunkManager.query(searchQuery, Duration.ofMillis(3000));
    assertThat(results.hits.size()).isEqualTo(2);
    assertThat(results.internalAggregation).isNotNull();
    assertThat(((StringTerms) results.internalAggregation).getBuckets().size()).isEqualTo(1);

    searchQuery = newTermsAggregatorCountSearchQuery("test1", "alerts");
    results = chunkManager.query(searchQuery, Duration.ofMillis(3000));
    assertThat(results.hits.size()).isEqualTo(2);
    assertThat(results.internalAggregation).isNotNull();
    assertThat(((UnmappedTerms) results.internalAggregation).getBuckets().size()).isEqualTo(0);
  }

  @Test
  public void testAddMessageParentdBeforeChild() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(
            metricsRegistry, 10 * 1024 * 1024 * 1024L, 1000000L);

    initChunkManager(chunkRollOverStrategy, blobStore, MoreExecutors.newDirectExecutorService());

    // parent span before child span
    List<Trace.Span> messages =
        Arrays.asList(
            SpanUtil.makeSpansCustomKeywordTags("alerts", "1", 1),
            SpanUtil.makeSpansCustomKeywordTags("alerts.count", "1", 2));

    int actualChunkSize = 0;
    int offset = 1;
    for (Trace.Span m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      actualChunkSize += msgSize;
      offset++;

      chunkManager.getActiveChunk().commit();
    }

    // ingestion works fine.
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(2);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(actualChunkSize);

    // aggregation on works for both; parent and child

    SearchQuery searchQuery = newTermsAggregatorCountSearchQuery("test1", "alerts.count");
    SearchResult<LogMessage> results = chunkManager.query(searchQuery, Duration.ofMillis(3000));
    assertThat(results.hits.size()).isEqualTo(2);
    assert results.internalAggregation != null;
    assertThat(((StringTerms) results.internalAggregation).getBuckets().size()).isEqualTo(1);

    searchQuery = newTermsAggregatorCountSearchQuery("test1", "alerts");
    results = chunkManager.query(searchQuery, Duration.ofMillis(3000));
    assertThat(results.hits.size()).isEqualTo(2);
    assert results.internalAggregation != null;
    assertThat(((StringTerms) results.internalAggregation).getBuckets().size()).isEqualTo(1);
  }
}
