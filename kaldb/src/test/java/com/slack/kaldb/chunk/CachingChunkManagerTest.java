package com.slack.kaldb.chunk;

import static com.slack.kaldb.config.KaldbConfig.REPLICA_STORE_ZK_PATH;
import static com.slack.kaldb.config.KaldbConfig.SNAPSHOT_METADATA_STORE_ZK_PATH;
import static com.slack.kaldb.logstore.BlobFsUtils.copyToS3;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_COUNTER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.addMessages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.blobfs.LocalBlobFs;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.manager.caching.CachingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.metadata.cache.CacheSlotMetadata;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.server.MetadataStoreService;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.lucene.index.IndexCommit;
import org.assertj.core.util.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class CachingChunkManagerTest {

  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl metadataStore;
  private MeterRegistry meterRegistry;

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  @Before
  public void startup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();
    CountingFatalErrorHandler countingFatalErrorHandler = new CountingFatalErrorHandler();
    metadataStore =
        new ZookeeperMetadataStoreImpl(
            testingServer.getConnectString(),
            "test",
            60000,
            1000,
            new RetryNTimes(1, 500),
            countingFatalErrorHandler,
            meterRegistry);
  }

  @After
  public void shutdown() throws IOException, TimeoutException {
    metadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  // todo - should this be part of the ReadOnlyChunkImplTest instead? probably.
  @Test
  public void shouldHandleChunkLivecycle() throws Exception {
    Tracing.newBuilder().build();

    String testBucket = "test-s3-bucket";

    KaldbConfigs.CacheConfig cacheConfig =
        KaldbConfigs.CacheConfig.newBuilder()
            .setSlotsPerInstance(3)
            .setDataDirectory("/tmp/shouldHandleChunkLivecycle/" + RandomStringUtils.random(10))
            .setServerConfig(
                KaldbConfigs.ServerConfig.newBuilder().setServerAddress("localhost").build())
            .build();

    KaldbConfigs.S3Config s3Config =
        KaldbConfigs.S3Config.newBuilder().setS3Bucket(testBucket).setS3Region("us-east-1").build();

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

    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(
            metadataStoreService.getMetadataStore(), REPLICA_STORE_ZK_PATH, false);
    String replicaId = "foo";
    String snapshotId = "bar";
    replicaMetadataStore
        .create(new ReplicaMetadata(replicaId, snapshotId))
        .get(5, TimeUnit.SECONDS);

    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(
            metadataStoreService.getMetadataStore(), SNAPSHOT_METADATA_STORE_ZK_PATH, false);
    snapshotMetadataStore
        .create(
            new SnapshotMetadata(
                snapshotId,
                "path",
                snapshotId,
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                1,
                "partitionId"))
        .get(5, TimeUnit.SECONDS);

    S3Client s3Client = S3_MOCK_RULE.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(testBucket).build());
    S3BlobFs s3BlobFs = new S3BlobFs();
    s3BlobFs.init(s3Client);

    LuceneIndexStoreImpl logStore =
        LuceneIndexStoreImpl.makeLogStore(
            Files.newTemporaryFolder(),
            Duration.ofSeconds(60),
            Duration.ofSeconds(60),
            meterRegistry);
    addMessages(logStore, 1, 10, true);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, meterRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(REFRESHES_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(getCount(COMMITS_COUNTER, meterRegistry)).isEqualTo(1);

    Path dirPath = logStore.getDirectory().toAbsolutePath();
    IndexCommit indexCommit = logStore.getIndexCommit();
    Collection<String> activeFiles = indexCommit.getFileNames();
    LocalBlobFs localBlobFs = new LocalBlobFs();

    logStore.close();
    assertThat(localBlobFs.listFiles(dirPath.toUri(), false).length)
        .isGreaterThanOrEqualTo(activeFiles.size());

    s3Client.createBucket(CreateBucketRequest.builder().bucket(s3Config.getS3Bucket()).build());

    // Copy files to S3.
    copyToS3(dirPath, activeFiles, s3Config.getS3Bucket(), snapshotId, s3BlobFs);

    CachingChunkManager<LogMessage> cachingChunkManager =
        new CachingChunkManager<>(meterRegistry, metadataStoreService, kaldbConfig, s3BlobFs);
    cachingChunkManager.startAsync();
    cachingChunkManager.awaitRunning(15, TimeUnit.SECONDS);

    ReadOnlyChunkImpl readOnlyChunk =
        (ReadOnlyChunkImpl) cachingChunkManager.getChunkMap().values().toArray()[0];

    // wait for chunk to register
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.FREE);

    CacheSlotMetadataStore cacheSlotMetadataStore = readOnlyChunk.cacheSlotMetadataStore;
    CacheSlotMetadata cacheSlotMetadata = cacheSlotMetadataStore.getCached().get(0);

    // update chunk to assigned
    CacheSlotMetadata updatedCacheSlotMetadata =
        new CacheSlotMetadata(
            cacheSlotMetadata.name,
            Metadata.CacheSlotState.ASSIGNED,
            replicaId,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.update(updatedCacheSlotMetadata);

    // ensure that the chunk was marked LIVE
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.LIVE);

    // query
    CompletableFuture<SearchResult<LogMessage>> result =
        cachingChunkManager.query(
            new SearchQuery(
                MessageUtil.TEST_INDEX_NAME,
                "*:*",
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                0));
    SearchResult<LogMessage> logMessageSearchResult = result.get(5, TimeUnit.SECONDS);
    assertThat(logMessageSearchResult.hits.size()).isEqualTo(10);

    // todo - assert remaining chunkInfos
    assertThat(readOnlyChunk.info().getNumDocs()).isEqualTo(10);

    // mark the chunk for eviction
    readOnlyChunk.setChunkMetadataState(Metadata.CacheSlotState.EVICT);

    // ensure that the evicted chunk was released
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.FREE);

    CompletableFuture<SearchResult<LogMessage>> emptyResult =
        cachingChunkManager.query(
            new SearchQuery(
                MessageUtil.TEST_INDEX_NAME,
                "*:*",
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                0));
    SearchResult<LogMessage> logMessageEmptySearchResult = emptyResult.get(5, TimeUnit.SECONDS);
    assertThat(logMessageEmptySearchResult.hits.size()).isEqualTo(0);

    // todo - assert remaining chunkInfos
    assertThat(readOnlyChunk.info()).isNull();

    cachingChunkManager.stopAsync();
    metadataStoreService.stopAsync();

    cachingChunkManager.awaitTerminated(15, TimeUnit.SECONDS);
    metadataStoreService.awaitTerminated(15, TimeUnit.SECONDS);
  }
}
