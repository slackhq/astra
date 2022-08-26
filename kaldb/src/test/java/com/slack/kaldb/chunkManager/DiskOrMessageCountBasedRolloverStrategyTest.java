package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.chunkManager.DiskOrMessageCountBasedRolloverStrategy.LIVE_BYTES_DIR;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.*;
import static com.slack.kaldb.testlib.ChunkManagerUtil.TEST_PORT;
import static com.slack.kaldb.testlib.MetricsUtil.getValue;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.curator.test.TestingServer;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class DiskOrMessageCountBasedRolloverStrategyTest {

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
  private SnapshotMetadataStore snapshotMetadataStore;
  private SearchMetadataStore searchMetadataStore;

  @Before
  public void setUp() throws Exception {
    DiskOrMessageCountBasedRolloverStrategy.DIRECTORY_SIZE_EXECUTOR_PERIOD_MS = 10;
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
    snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    searchMetadataStore = new SearchMetadataStore(metadataStore, false);
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
      ListeningExecutorService listeningExecutorService)
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
            metadataStore,
            searchContext,
            KaldbConfigUtil.makeIndexerConfig(TEST_PORT, 1000, "log_message", 100));
    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
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
  public void testDiskBasedRolloverWithMultipleChunks() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10000, 1_000_000_000);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 10);
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
      shouldCheckOnNextMessage = getValue(LIVE_BYTES_DIR, metricsRegistry) > 10000;
    }
  }
}
