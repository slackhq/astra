package com.slack.kaldb.testlib;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.ChunkManager;
import com.slack.kaldb.chunk.ChunkRollOverStrategy;
import com.slack.kaldb.chunk.ChunkRollOverStrategyImpl;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
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

  public ChunkManagerUtil(
      S3MockRule s3MockRule,
      MeterRegistry meterRegistry,
      long maxBytesPerChunk,
      long maxMessagesPerChunk)
      throws TimeoutException {

    tempFolder = Files.createTempDir(); // TODO: don't use beta func.
    // create an S3 client and a bucket for test
    s3Client = s3MockRule.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_TEST_BUCKET).build());

    S3BlobFs s3BlobFs = new S3BlobFs();
    s3BlobFs.init(s3Client);

    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(maxBytesPerChunk, maxMessagesPerChunk);

    chunkManager =
        new ChunkManager<>(
            "testData",
            tempFolder.getAbsolutePath(),
            chunkRollOverStrategy,
            meterRegistry,
            s3BlobFs,
            S3_TEST_BUCKET,
            MoreExecutors.newDirectExecutorService(),
            10000);
    chunkManager.startAsync();
    chunkManager.awaitRunning(15, TimeUnit.SECONDS);
  }

  public ChunkManagerUtil(S3MockRule s3MockRule, MeterRegistry meterRegistry)
      throws TimeoutException {
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
    FileUtils.deleteDirectory(tempFolder);
  }
}
