package com.slack.kaldb.testlib;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.config.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.chunkManager.ChunkRollOverStrategy;
import com.slack.kaldb.chunkManager.ChunkRollOverStrategyImpl;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * This class creates a chunk manager that can be used in unit tests.
 *
 * <p>TODO: Convert this class into a test rule to make it easy to use.
 */
public class ChunkManagerUtil<T> {

  public static final String TEST_HOST = "localhost";
  public static final int TEST_PORT = 34567;

  private final File tempFolder;
  public final S3Client s3Client;
  public static final String S3_TEST_BUCKET = "test-kaldb-logs";
  public static final String ZK_PATH_PREFIX = "testZK";
  public final IndexingChunkManager<T> chunkManager;
  private final TestingServer localZkServer;
  private final MetadataStore metadataStore;

  public ChunkManagerUtil(
      S3MockRule s3MockRule,
      MeterRegistry meterRegistry,
      long maxBytesPerChunk,
      long maxMessagesPerChunk)
      throws Exception {
    this(
        s3MockRule,
        meterRegistry,
        maxBytesPerChunk,
        maxMessagesPerChunk,
        new SearchContext("localhost", TEST_PORT));
  }

  public ChunkManagerUtil(
      S3MockRule s3MockRule,
      MeterRegistry meterRegistry,
      long maxBytesPerChunk,
      long maxMessagesPerChunk,
      SearchContext searchContext)
      throws Exception {

    tempFolder = Files.createTempDir(); // TODO: don't use beta func.
    // create an S3 client and a bucket for test
    s3Client = s3MockRule.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_TEST_BUCKET).build());

    S3BlobFs s3BlobFs = new S3BlobFs();
    s3BlobFs.init(s3Client);

    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(maxBytesPerChunk, maxMessagesPerChunk);

    localZkServer = new TestingServer();
    localZkServer.start();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(localZkServer.getConnectString())
            .setZkPathPrefix(ZK_PATH_PREFIX)
            .setZkSessionTimeoutMs(15000)
            .setZkConnectionTimeoutMs(15000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);

    chunkManager =
        new IndexingChunkManager<>(
            "testData",
            tempFolder.getAbsolutePath(),
            chunkRollOverStrategy,
            meterRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            MoreExecutors.newDirectExecutorService(),
            10000,
            metadataStore,
            searchContext);
    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  public void close() throws IOException, TimeoutException {
    chunkManager.stopAsync();
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    s3Client.close();
    metadataStore.close();
    localZkServer.close();
    FileUtils.deleteDirectory(tempFolder);
  }

  public static List<SnapshotMetadata> fetchNonLiveSnapshot(List<SnapshotMetadata> snapshots) {
    Predicate<SnapshotMetadata> nonLiveSnapshotPredicate =
        s -> !s.snapshotPath.equals(SearchMetadata.LIVE_SNAPSHOT_PATH);
    return fetchSnapshotMatching(snapshots, nonLiveSnapshotPredicate);
  }

  public static List<SnapshotMetadata> fetchLiveSnapshot(List<SnapshotMetadata> snapshots) {
    Predicate<SnapshotMetadata> liveSnapshotPredicate =
        s -> s.snapshotPath.equals(SearchMetadata.LIVE_SNAPSHOT_PATH);
    return fetchSnapshotMatching(snapshots, liveSnapshotPredicate);
  }

  public static List<SnapshotMetadata> fetchSnapshotMatching(
      List<SnapshotMetadata> afterSnapshots, Predicate<SnapshotMetadata> condition) {
    return afterSnapshots.stream().filter(condition).collect(Collectors.toList());
  }

  public static List<SearchMetadata> fetchSearchNodes(IndexingChunkManager<LogMessage> chunkManager)
      throws InterruptedException, ExecutionException, TimeoutException {
    return chunkManager
        .getSearchMetadataStore()
        .list()
        .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
  }

  public static List<SnapshotMetadata> fetchSnapshots(IndexingChunkManager<LogMessage> chunkManager)
      throws InterruptedException, ExecutionException, TimeoutException {
    return chunkManager
        .getSnapshotMetadataStore()
        .list()
        .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
  }
}
