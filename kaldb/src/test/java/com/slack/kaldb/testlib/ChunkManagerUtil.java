package com.slack.kaldb.testlib;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.ChunkManager;
import com.slack.kaldb.chunk.ChunkRollOverStrategy;
import com.slack.kaldb.chunk.ChunkRollOverStrategyImpl;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
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
  public final ChunkManager<T> chunkManager;
  private final TestingServer localZkServer;
  private final MetadataStoreService metadataStoreService;

  public ChunkManagerUtil(
      S3MockRule s3MockRule,
      MeterRegistry meterRegistry,
      long maxBytesPerChunk,
      long maxMessagesPerChunk)
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

    metadataStoreService = new MetadataStoreService(meterRegistry);
    metadataStoreService.startAsync();

    chunkManager =
        new ChunkManager<>(
            "testData",
            tempFolder.getAbsolutePath(),
            chunkRollOverStrategy,
            meterRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            MoreExecutors.newDirectExecutorService(),
            10000,
            metadataStoreService);
    chunkManager.startAsync();
    chunkManager.awaitRunning(15, TimeUnit.SECONDS);
  }

  public ChunkManagerUtil(S3MockRule s3MockRule, MeterRegistry meterRegistry) throws Exception {
    this(s3MockRule, meterRegistry, 10 * 1024 * 1024 * 1024L, 10L);
  }

  public void close() throws IOException, TimeoutException {
    if (chunkManager != null) {
      chunkManager.stopAsync();
      chunkManager.awaitTerminated(15, TimeUnit.SECONDS);
    }
    if (s3Client != null) {
      s3Client.close();
    }
    metadataStoreService.stopAsync();
    metadataStoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);
    if (localZkServer != null) {
      localZkServer.close();
    }
    FileUtils.deleteDirectory(tempFolder);
  }
}
