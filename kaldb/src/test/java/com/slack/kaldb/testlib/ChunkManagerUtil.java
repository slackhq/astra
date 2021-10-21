package com.slack.kaldb.testlib;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.chunkManager.ChunkRollOverStrategy;
import com.slack.kaldb.chunkManager.ChunkRollOverStrategyImpl;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
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

  private final File tempFolder;
  public final S3Client s3Client;
  public static final String S3_TEST_BUCKET = "test-kaldb-logs";
  public static final String ZK_PATH_PREFIX = "testZK";
  public final IndexingChunkManager<T> chunkManager;
  private final TestingServer localZkServer;
  private final MetadataStoreService metadataStoreService;

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
        new SearchContext("localhost", 10009));
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

    metadataStoreService = new MetadataStoreService(meterRegistry, zkConfig);
    metadataStoreService.startAsync();

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
            metadataStoreService,
            searchContext);
    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  public void close() throws IOException, TimeoutException {
    chunkManager.stopAsync();
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    s3Client.close();
    metadataStoreService.stopAsync();
    metadataStoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);
    localZkServer.close();
    FileUtils.deleteDirectory(tempFolder);
  }
}
