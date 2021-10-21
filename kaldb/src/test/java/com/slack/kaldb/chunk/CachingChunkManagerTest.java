package com.slack.kaldb.chunk;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunkManager.CachingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class CachingChunkManagerTest {
  private final String TEST_S3_BUCKET =
      String.format("%sBucket", this.getClass().getSimpleName()).toLowerCase();

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private S3BlobFs s3BlobFs;

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  @Before
  public void startup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    S3Client s3Client = S3_MOCK_RULE.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(TEST_S3_BUCKET).build());
    s3BlobFs = new S3BlobFs();
    s3BlobFs.init(s3Client);
  }

  @After
  public void shutdown() throws IOException {
    s3BlobFs.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldHandleLifecycle() throws Exception {
    KaldbConfigs.CacheConfig cacheConfig =
        KaldbConfigs.CacheConfig.newBuilder()
            .setSlotsPerInstance(3)
            .setDataDirectory(
                String.format(
                    "/tmp/%s/%s", this.getClass().getSimpleName(), RandomStringUtils.random(10)))
            .setServerConfig(
                KaldbConfigs.ServerConfig.newBuilder()
                    .setServerAddress("localhost")
                    .setServerPort(8080)
                    .build())
            .build();

    KaldbConfigs.S3Config s3Config =
        KaldbConfigs.S3Config.newBuilder()
            .setS3Bucket(TEST_S3_BUCKET)
            .setS3Region("us-east-1")
            .build();

    KaldbConfigs.KaldbConfig kaldbConfig =
        KaldbConfigs.KaldbConfig.newBuilder()
            .setCacheConfig(cacheConfig)
            .setS3Config(s3Config)
            .build();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    MetadataStoreService metadataStoreService = new MetadataStoreService(meterRegistry, zkConfig);
    metadataStoreService.startAsync();
    metadataStoreService.awaitRunning(15, TimeUnit.SECONDS);

    CachingChunkManager<LogMessage> cachingChunkManager =
        new CachingChunkManager<>(
            meterRegistry,
            metadataStoreService,
            s3BlobFs,
            SearchContext.fromConfig(kaldbConfig.getCacheConfig().getServerConfig()),
            kaldbConfig.getS3Config().getS3Bucket(),
            kaldbConfig.getCacheConfig().getDataDirectory(),
            kaldbConfig.getCacheConfig().getSlotsPerInstance());

    cachingChunkManager.startAsync();
    cachingChunkManager.awaitRunning(15, TimeUnit.SECONDS);

    assertThat(cachingChunkManager.getChunkList().size()).isEqualTo(3);

    List<Chunk<LogMessage>> readOnlyChunks = cachingChunkManager.getChunkList();
    await()
        .until(
            () ->
                ((ReadOnlyChunkImpl) (readOnlyChunks.get(0)))
                    .getChunkMetadataState()
                    .equals(Metadata.CacheSlotState.FREE));
    await()
        .until(
            () ->
                ((ReadOnlyChunkImpl) (readOnlyChunks.get(1)))
                    .getChunkMetadataState()
                    .equals(Metadata.CacheSlotState.FREE));
    await()
        .until(
            () ->
                ((ReadOnlyChunkImpl) (readOnlyChunks.get(2)))
                    .getChunkMetadataState()
                    .equals(Metadata.CacheSlotState.FREE));

    cachingChunkManager.stopAsync();
    metadataStoreService.stopAsync();

    cachingChunkManager.awaitTerminated(15, TimeUnit.SECONDS);
    metadataStoreService.awaitTerminated(15, TimeUnit.SECONDS);
  }
}
