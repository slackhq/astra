package com.slack.kaldb.chunk;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.config.KaldbConfig.REPLICA_STORE_ZK_PATH;
import static com.slack.kaldb.config.KaldbConfig.SEARCH_METADATA_STORE_ZK_PATH;
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
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.metadata.cache.CacheSlotMetadata;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.server.MetadataStoreService;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.lucene.index.IndexCommit;
import org.assertj.core.util.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class ReadOnlyChunkImplTest {
  private final String TEST_S3_BUCKET =
      String.format("%sBucket", this.getClass().getSimpleName()).toLowerCase();

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private S3BlobFs s3BlobFs;

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  @Before
  public void startup() throws Exception {
    Tracing.newBuilder().build();
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
  public void shouldHandleChunkLivecycle() throws Exception {
    KaldbConfigs.KaldbConfig kaldbConfig = getKaldbConfig();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleChunkLivecycle")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    MetadataStoreService metadataStoreService = new MetadataStoreService(meterRegistry, zkConfig);
    metadataStoreService.startAsync();
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(
            metadataStoreService.getMetadataStore(), REPLICA_STORE_ZK_PATH, false);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(
            metadataStoreService.getMetadataStore(), SNAPSHOT_METADATA_STORE_ZK_PATH, false);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(
            metadataStoreService.getMetadataStore(), SEARCH_METADATA_STORE_ZK_PATH, true);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(metadataStoreService, replicaId, snapshotId);
    initializeZkSnapshot(metadataStoreService, snapshotId);
    initializeBlobStorageWithIndex(snapshotId);

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl(
            metadataStoreService,
            meterRegistry,
            s3BlobFs,
            SearchContext.fromConfig(kaldbConfig.getCacheConfig().getServerConfig()),
            kaldbConfig.getS3Config().getS3Bucket(),
            kaldbConfig.getCacheConfig().getDataDirectory(),
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.FREE);

    assignReplicaToChunk(replicaId, readOnlyChunk);

    // ensure that the chunk was marked LIVE
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.LIVE);

    SearchResult<LogMessage> logMessageSearchResult =
        readOnlyChunk.query(
            new SearchQuery(
                MessageUtil.TEST_INDEX_NAME,
                "*:*",
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                0));
    assertThat(logMessageSearchResult.hits.size()).isEqualTo(10);
    assertThat(readOnlyChunk.successfulChunkAssignments.count()).isEqualTo(1);

    // ensure we registered a search node for this cache slot
    await().until(() -> searchMetadataStore.getCached().size() == 1);
    assertThat(searchMetadataStore.getCached().get(0).snapshotName).isEqualTo(snapshotId);
    assertThat(searchMetadataStore.getCached().get(0).url).isEqualTo("localhost:8080");
    assertThat(searchMetadataStore.getCached().get(0).name).isEqualTo("localhost");

    // todo - consider adding additional chunkInfo validations
    assertThat(readOnlyChunk.info().getNumDocs()).isEqualTo(10);

    // mark the chunk for eviction
    readOnlyChunk.setChunkMetadataState(Metadata.CacheSlotState.EVICT);

    // ensure that the evicted chunk was released
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.FREE);

    // ensure the search metadata node was unregistered
    await().until(() -> searchMetadataStore.getCached().size() == 0);

    SearchResult<LogMessage> logMessageEmptySearchResult =
        readOnlyChunk.query(
            new SearchQuery(
                MessageUtil.TEST_INDEX_NAME,
                "*:*",
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                0));
    assertThat(logMessageEmptySearchResult).isEqualTo(SearchResult.empty());
    assertThat(readOnlyChunk.info()).isNull();
    assertThat(readOnlyChunk.successfulChunkEvictions.count()).isEqualTo(1);

    assertThat(readOnlyChunk.failedChunkAssignments.count()).isEqualTo(0);

    metadataStoreService.stopAsync();
    metadataStoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldHandleMissingS3Assets() throws Exception {
    KaldbConfigs.KaldbConfig kaldbConfig = getKaldbConfig();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleMissingS3Assets")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    MetadataStoreService metadataStoreService = new MetadataStoreService(meterRegistry, zkConfig);
    metadataStoreService.startAsync();
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(
            metadataStoreService.getMetadataStore(), REPLICA_STORE_ZK_PATH, false);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(
            metadataStoreService.getMetadataStore(), SNAPSHOT_METADATA_STORE_ZK_PATH, false);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(
            metadataStoreService.getMetadataStore(), SEARCH_METADATA_STORE_ZK_PATH, true);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(metadataStoreService, replicaId, snapshotId);
    initializeZkSnapshot(metadataStoreService, snapshotId);

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl(
            metadataStoreService,
            meterRegistry,
            s3BlobFs,
            SearchContext.fromConfig(kaldbConfig.getCacheConfig().getServerConfig()),
            kaldbConfig.getS3Config().getS3Bucket(),
            kaldbConfig.getCacheConfig().getDataDirectory(),
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.FREE);

    assignReplicaToChunk(replicaId, readOnlyChunk);

    // assert that the chunk was released back to free
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.FREE);

    // ensure we did not register a search node
    assertThat(searchMetadataStore.getCached().size()).isEqualTo(0);

    assertThat(readOnlyChunk.successfulChunkAssignments.count()).isEqualTo(0);
    assertThat(readOnlyChunk.failedChunkAssignments.count()).isEqualTo(1);

    metadataStoreService.stopAsync();
    metadataStoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldHandleMissingZkData() throws Exception {
    KaldbConfigs.KaldbConfig kaldbConfig = getKaldbConfig();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleMissingZkData")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    MetadataStoreService metadataStoreService = new MetadataStoreService(meterRegistry, zkConfig);
    metadataStoreService.startAsync();
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(
            metadataStoreService.getMetadataStore(), REPLICA_STORE_ZK_PATH, false);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(
            metadataStoreService.getMetadataStore(), SNAPSHOT_METADATA_STORE_ZK_PATH, false);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(
            metadataStoreService.getMetadataStore(), SEARCH_METADATA_STORE_ZK_PATH, true);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(metadataStoreService, replicaId, snapshotId);
    // we intentionally do not initialize a Snapshot, so the lookup is expected to fail

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl(
            metadataStoreService,
            meterRegistry,
            s3BlobFs,
            SearchContext.fromConfig(kaldbConfig.getCacheConfig().getServerConfig()),
            kaldbConfig.getS3Config().getS3Bucket(),
            kaldbConfig.getCacheConfig().getDataDirectory(),
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.FREE);

    assignReplicaToChunk(replicaId, readOnlyChunk);

    // assert that the chunk was released back to free
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.FREE);

    // ensure we did not register a search node
    assertThat(searchMetadataStore.getCached().size()).isEqualTo(0);

    assertThat(readOnlyChunk.successfulChunkAssignments.count()).isEqualTo(0);
    assertThat(readOnlyChunk.failedChunkAssignments.count()).isEqualTo(1);

    metadataStoreService.stopAsync();
    metadataStoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void closeShouldCleanupLiveChunkCorrectly() throws Exception {
    KaldbConfigs.KaldbConfig kaldbConfig = getKaldbConfig();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleChunkLivecycle")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    MetadataStoreService metadataStoreService = new MetadataStoreService(meterRegistry, zkConfig);
    metadataStoreService.startAsync();
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(
            metadataStoreService.getMetadataStore(), REPLICA_STORE_ZK_PATH, false);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(
            metadataStoreService.getMetadataStore(), SNAPSHOT_METADATA_STORE_ZK_PATH, false);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(
            metadataStoreService.getMetadataStore(), SEARCH_METADATA_STORE_ZK_PATH, true);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(metadataStoreService, replicaId, snapshotId);
    initializeZkSnapshot(metadataStoreService, snapshotId);
    initializeBlobStorageWithIndex(snapshotId);

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl(
            metadataStoreService,
            meterRegistry,
            s3BlobFs,
            SearchContext.fromConfig(kaldbConfig.getCacheConfig().getServerConfig()),
            kaldbConfig.getS3Config().getS3Bucket(),
            kaldbConfig.getCacheConfig().getDataDirectory(),
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.FREE);

    assignReplicaToChunk(replicaId, readOnlyChunk);

    // ensure that the chunk was marked LIVE
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.LIVE);

    SearchQuery query =
        new SearchQuery(
            MessageUtil.TEST_INDEX_NAME,
            "*:*",
            Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().toEpochMilli(),
            500,
            0);
    SearchResult<LogMessage> logMessageSearchResult = readOnlyChunk.query(query);
    assertThat(logMessageSearchResult.hits.size()).isEqualTo(10);
    assertThat(readOnlyChunk.successfulChunkAssignments.count()).isEqualTo(1);

    // ensure we registered a search node for this cache slot
    await().until(() -> searchMetadataStore.getCached().size() == 1);
    assertThat(searchMetadataStore.getCached().get(0).snapshotName).isEqualTo(snapshotId);
    assertThat(searchMetadataStore.getCached().get(0).url).isEqualTo("localhost:8080");
    assertThat(searchMetadataStore.getCached().get(0).name).isEqualTo("localhost");

    // verify we have files on disk
    assertThat(java.nio.file.Files.list(readOnlyChunk.getDataDirectory()).findFirst().isPresent())
        .isTrue();

    // attempt to close the readOnlyChunk
    readOnlyChunk.close();

    // verify no results are returned for the exact same query we did above
    SearchResult<LogMessage> logMessageSearchResultEmpty = readOnlyChunk.query(query);
    assertThat(logMessageSearchResultEmpty).isEqualTo(SearchResult.empty());
    assertThat(readOnlyChunk.info()).isNull();

    // verify that the directory has been cleaned up
    assertThat(java.nio.file.Files.list(readOnlyChunk.getDataDirectory()).findFirst().isPresent())
        .isFalse();

    metadataStoreService.stopAsync();
    metadataStoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  private void assignReplicaToChunk(String replicaId, ReadOnlyChunkImpl<LogMessage> readOnlyChunk) {
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
  }

  private void initializeZkSnapshot(MetadataStoreService metadataStoreService, String snapshotId)
      throws Exception {
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
  }

  private void initializeZkReplica(
      MetadataStoreService metadataStoreService, String replicaId, String snapshotId)
      throws Exception {
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(
            metadataStoreService.getMetadataStore(), REPLICA_STORE_ZK_PATH, false);
    replicaMetadataStore
        .create(new ReplicaMetadata(replicaId, snapshotId))
        .get(5, TimeUnit.SECONDS);
  }

  private void initializeBlobStorageWithIndex(String snapshotId) throws Exception {
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

    // Copy files to S3.
    copyToS3(dirPath, activeFiles, TEST_S3_BUCKET, snapshotId, s3BlobFs);
  }

  private KaldbConfigs.KaldbConfig getKaldbConfig() {
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

    return KaldbConfigs.KaldbConfig.newBuilder()
        .setCacheConfig(cacheConfig)
        .setS3Config(s3Config)
        .build();
  }
}
