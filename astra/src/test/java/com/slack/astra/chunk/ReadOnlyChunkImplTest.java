package com.slack.astra.chunk;

import static com.slack.astra.chunk.ReadOnlyChunkImpl.CHUNK_ASSIGNMENT_TIMER;
import static com.slack.astra.chunk.ReadOnlyChunkImpl.CHUNK_EVICTION_TIMER;
import static com.slack.astra.chunk.ReadWriteChunk.SCHEMA_FILE_NAME;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.MetricsUtil.getTimerCount;
import static com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension.addMessages;
import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LuceneIndexStoreImpl;
import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.cache.CacheSlotMetadata;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.replica.ReplicaMetadata;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.schema.ChunkSchema;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.util.QueryBuilderUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.lucene.index.IndexCommit;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class ReadOnlyChunkImplTest {
  private static final String TEST_S3_BUCKET = "read-only-chunk-impl-test";

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private BlobStore blobStore;

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(TEST_S3_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  @BeforeEach
  public void startup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    S3AsyncClient s3AsyncClient =
        S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    blobStore = new BlobStore(s3AsyncClient, TEST_S3_BUCKET);
  }

  @AfterEach
  public void shutdown() throws IOException {
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldHandleChunkLivecycle() throws Exception {
    AstraConfigs.AstraConfig AstraConfig = makeCacheConfig();
    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("shouldHandleChunkLivecycle")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    AsyncCuratorFramework curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
    CacheSlotMetadataStore cacheSlotMetadataStore =
        new CacheSlotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    CacheNodeAssignmentStore cacheNodeAssignmentStore =
        new CacheNodeAssignmentStore(curatorFramework, metadataStoreConfig, meterRegistry);
    CacheNodeMetadataStore cacheNodeMetadataStore =
        new CacheNodeMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);

    String replicaId = "foo";
    String snapshotId = "boo";
    String assignmentId = "dog";
    String cacheNodeId = "baz";
    String replicaSet = "cat";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, metadataStoreConfig, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, metadataStoreConfig, snapshotId, 0);
    initializeBlobStorageWithIndex(snapshotId);
    initializeCacheNodeAssignment(
        cacheNodeAssignmentStore, assignmentId, snapshotId, cacheNodeId, replicaSet, replicaId);
    initializeCacheNode(cacheNodeMetadataStore, cacheNodeId, "some-host.name", 1, replicaSet, true);

    SearchContext searchContext =
        SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig());
    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            blobStore,
            searchContext,
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore,
            cacheNodeAssignmentStore,
            cacheNodeAssignmentStore.getSync(cacheNodeId, assignmentId),
            snapshotMetadataStore.findSync(snapshotId),
            cacheNodeMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // ensure that the chunk was marked LIVE
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);
    assertThat(readOnlyChunk.getChunkMetadataState())
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.LIVE);

    SearchResult<LogMessage> logMessageSearchResult =
        readOnlyChunk.query(
            new SearchQuery(
                MessageUtil.TEST_DATASET_NAME,
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                Collections.emptyList(),
                QueryBuilderUtil.generateQueryBuilder(
                    "*:*",
                    Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                    Instant.now().toEpochMilli()),
                null,
                createGenericDateHistogramAggregatorFactoriesBuilder()));
    assertThat(logMessageSearchResult.hits.size()).isEqualTo(10);

    await()
        .until(
            () ->
                meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count()
                    == 1);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);

    // ensure we registered a search node for this cache slot
    await().until(() -> searchMetadataStore.listSync().size() == 1);
    assertThat(searchMetadataStore.listSync().get(0).snapshotName).isEqualTo(snapshotId);

    assertThat(searchMetadataStore.listSync().get(0).url).isEqualTo("gproto+http://localhost:8080");
    assertThat(searchMetadataStore.listSync().get(0).name)
        .isEqualTo(SearchMetadata.generateSearchContextSnapshotId(snapshotId, "localhost"));

    // mark the chunk for eviction
    CacheSlotMetadata cacheSlotMetadata =
        cacheSlotMetadataStore.getSync(searchContext.hostname, readOnlyChunk.slotId);
    cacheSlotMetadataStore
        .updateNonFreeCacheSlotState(
            cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.EVICT)
        .get(1, TimeUnit.SECONDS);

    // ensure that the evicted chunk was released
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    // ensure the search metadata node was unregistered
    await().until(() -> searchMetadataStore.listSync().size() == 0);

    SearchResult<LogMessage> logMessageEmptySearchResult =
        readOnlyChunk.query(
            new SearchQuery(
                MessageUtil.TEST_DATASET_NAME,
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                Collections.emptyList(),
                QueryBuilderUtil.generateQueryBuilder(
                    "*:*",
                    Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                    Instant.now().toEpochMilli()),
                null,
                createGenericDateHistogramAggregatorFactoriesBuilder()));
    assertThat(logMessageEmptySearchResult).isEqualTo(SearchResult.empty());
    assertThat(readOnlyChunk.info()).isNull();

    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(1);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(1);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);

    curatorFramework.unwrap().close();
  }

  @Test
  public void shouldHandleMissingS3Assets() throws Exception {
    AstraConfigs.AstraConfig AstraConfig = makeCacheConfig();
    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("shouldHandleMissingS3Assets")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    AsyncCuratorFramework curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
    CacheSlotMetadataStore cacheSlotMetadataStore =
        new CacheSlotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    CacheNodeMetadataStore cacheNodeMetadataStore =
        new CacheNodeMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);

    String replicaId = "foo";
    String snapshotId = "bar";
    String cacheNodeId = "baz";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, metadataStoreConfig, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, metadataStoreConfig, snapshotId, 0);
    initializeCacheNode(cacheNodeMetadataStore, cacheNodeId, "some-host.name", 1, "rep1", true);

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            blobStore,
            SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig()),
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore,
            cacheNodeMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // assert that the chunk was released back to free
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    // ensure we did not register a search node
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(0);

    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(1);

    curatorFramework.unwrap().close();
  }

  @Test
  public void shouldHandleMissingZkData() throws Exception {
    AstraConfigs.AstraConfig AstraConfig = makeCacheConfig();
    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("shouldHandleMissingZkData")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    AsyncCuratorFramework curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
    CacheSlotMetadataStore cacheSlotMetadataStore =
        new CacheSlotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    CacheNodeMetadataStore cacheNodeMetadataStore =
        new CacheNodeMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);

    String replicaId = "foo";
    String snapshotId = "bar";
    String cacheNodeId = "baz";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, metadataStoreConfig, replicaId, snapshotId);
    initializeCacheNode(cacheNodeMetadataStore, cacheNodeId, "some-host.name", 1, "rep1", true);
    // we intentionally do not initialize a Snapshot, so the lookup is expected to fail

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            blobStore,
            SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig()),
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore,
            cacheNodeMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // assert that the chunk was released back to free
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    // ensure we did not register a search node
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(0);

    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(1);

    curatorFramework.unwrap().close();
  }

  @Test
  public void closeShouldCleanupLiveChunkCorrectly() throws Exception {
    AstraConfigs.AstraConfig AstraConfig = makeCacheConfig();
    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("shouldHandleChunkLivecycle")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    AsyncCuratorFramework curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
    CacheSlotMetadataStore cacheSlotMetadataStore =
        new CacheSlotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    CacheNodeMetadataStore cacheNodeMetadataStore =
        new CacheNodeMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    CacheNodeAssignmentStore cacheNodeAssignmentStore =
        new CacheNodeAssignmentStore(curatorFramework, metadataStoreConfig, meterRegistry);

    String replicaId = "foo";
    String snapshotId = "bar";
    String cacheNodeId = "baz";
    String replicaSet = "cat";
    String assignmentId = "dog";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, metadataStoreConfig, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, metadataStoreConfig, snapshotId, 0);
    initializeBlobStorageWithIndex(snapshotId);
    initializeCacheNodeAssignment(
        cacheNodeAssignmentStore, assignmentId, snapshotId, cacheNodeId, replicaSet, replicaId);
    initializeCacheNode(cacheNodeMetadataStore, cacheNodeId, "some-host.name", 1, "rep1", true);

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            blobStore,
            SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig()),
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore,
            cacheNodeAssignmentStore,
            cacheNodeAssignmentStore.getSync(cacheNodeId, assignmentId),
            snapshotMetadataStore.findSync(snapshotId),
            cacheNodeMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // ensure that the chunk was marked LIVE
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.LIVE);

    SearchQuery query =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().toEpochMilli(),
            500,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder(
                "*:*",
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    SearchResult<LogMessage> logMessageSearchResult = readOnlyChunk.query(query);
    assertThat(logMessageSearchResult.hits.size()).isEqualTo(10);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(1);

    // ensure we registered a search node for this cache slot
    await().until(() -> searchMetadataStore.listSync().size() == 1);
    assertThat(searchMetadataStore.listSync().get(0).snapshotName).isEqualTo(snapshotId);

    assertThat(searchMetadataStore.listSync().get(0).url).isEqualTo("gproto+http://localhost:8080");
    assertThat(searchMetadataStore.listSync().get(0).name)
        .isEqualTo(SearchMetadata.generateSearchContextSnapshotId(snapshotId, "localhost"));

    // verify we have files on disk
    try (var files = java.nio.file.Files.list(readOnlyChunk.getDataDirectory())) {
      assertThat(files.findFirst().isPresent()).isTrue();
    }

    // attempt to close the readOnlyChunk
    readOnlyChunk.close();

    // verify no results are returned for the exact same query we did above
    SearchResult<LogMessage> logMessageSearchResultEmpty = readOnlyChunk.query(query);
    assertThat(logMessageSearchResultEmpty).isEqualTo(SearchResult.empty());
    assertThat(readOnlyChunk.info()).isNull();

    // verify that the directory has been cleaned up
    try (var files = java.nio.file.Files.list(readOnlyChunk.getDataDirectory())) {
      assertThat(files.findFirst().isPresent()).isFalse();
    }

    curatorFramework.unwrap().close();
  }

  @Test
  public void shouldHandleDynamicChunkSizeLifecycle() throws Exception {
    AstraConfigs.AstraConfig AstraConfig = makeCacheConfig();
    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("shouldHandleChunkLivecycle")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    AsyncCuratorFramework curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
    CacheSlotMetadataStore cacheSlotMetadataStore =
        new CacheSlotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    CacheNodeAssignmentStore cacheNodeAssignmentStore =
        new CacheNodeAssignmentStore(curatorFramework, metadataStoreConfig, meterRegistry);
    CacheNodeMetadataStore cacheNodeMetadataStore =
        new CacheNodeMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);

    String replicaId = "foo";
    String snapshotId = "boo";
    String assignmentId = "dog";
    String cacheNodeId = "baz";
    String replicaSet = "cat";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, metadataStoreConfig, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, metadataStoreConfig, snapshotId, 29);
    initializeBlobStorageWithIndex(snapshotId);
    initializeCacheNodeAssignment(
        cacheNodeAssignmentStore, assignmentId, snapshotId, cacheNodeId, replicaSet, replicaId);
    initializeCacheNode(cacheNodeMetadataStore, cacheNodeId, "some-host.name", 1, "rep1", true);

    SearchContext searchContext =
        SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig());
    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            blobStore,
            searchContext,
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore,
            cacheNodeAssignmentStore,
            cacheNodeAssignmentStore.getSync(cacheNodeId, assignmentId),
            snapshotMetadataStore.findSync(snapshotId),
            cacheNodeMetadataStore);

    // wait for chunk to register
    // ignoreExceptions is workaround for https://github.com/aws/aws-sdk-java-v2/issues/3658
    await()
        .ignoreExceptions()
        .until(
            () -> {
              Path dataDirectory =
                  Path.of(
                      String.format(
                          "%s/astra-chunk-%s",
                          AstraConfig.getCacheConfig().getDataDirectory(), assignmentId));

              if (java.nio.file.Files.isDirectory(dataDirectory)) {
                FileUtils.cleanDirectory(dataDirectory.toFile());
              }
              readOnlyChunk.downloadChunkData();

              return cacheNodeAssignmentStore.getSync(
                          readOnlyChunk.getCacheNodeAssignment().cacheNodeId,
                          readOnlyChunk.getCacheNodeAssignment().assignmentId)
                      .state
                  == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE;
            });

    SearchResult<LogMessage> logMessageSearchResult =
        readOnlyChunk.query(
            new SearchQuery(
                MessageUtil.TEST_DATASET_NAME,
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                Collections.emptyList(),
                QueryBuilderUtil.generateQueryBuilder(
                    "*:*",
                    Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                    Instant.now().toEpochMilli()),
                null,
                createGenericDateHistogramAggregatorFactoriesBuilder()));
    assertThat(logMessageSearchResult.hits.size()).isEqualTo(10);

    // ensure we registered a search node for this cache assignment
    await().until(() -> searchMetadataStore.listSync().size() == 1);
    assertThat(searchMetadataStore.listSync().get(0).snapshotName).isEqualTo(snapshotId);

    assertThat(searchMetadataStore.listSync().get(0).url).isEqualTo("gproto+http://localhost:8080");
    assertThat(searchMetadataStore.listSync().get(0).name)
        .isEqualTo(SearchMetadata.generateSearchContextSnapshotId(snapshotId, "localhost"));

    // simulate eviction
    readOnlyChunk.evictChunk(cacheNodeAssignmentStore.findSync(assignmentId));

    // verify that the directory has been cleaned up
    try (var files = java.nio.file.Files.list(readOnlyChunk.getDataDirectory())) {
      assertThat(files.findFirst().isPresent()).isFalse();
    }

    curatorFramework.unwrap().close();
  }

  private void assignReplicaToChunk(
      CacheSlotMetadataStore cacheSlotMetadataStore,
      String replicaId,
      ReadOnlyChunkImpl<LogMessage> readOnlyChunk) {
    // update chunk to assigned
    CacheSlotMetadata updatedCacheSlotMetadata =
        new CacheSlotMetadata(
            readOnlyChunk.slotId,
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaId,
            Instant.now().toEpochMilli(),
            readOnlyChunk.searchContext.hostname,
            "rep1");
    cacheSlotMetadataStore.updateAsync(updatedCacheSlotMetadata);
  }

  private void initializeZkSnapshot(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      String snapshotId,
      long sizeInBytesOnDisk)
      throws Exception {
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    snapshotMetadataStore.createSync(
        new SnapshotMetadata(
            snapshotId,
            Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().toEpochMilli(),
            1,
            "partitionId",
            sizeInBytesOnDisk));
  }

  private void initializeZkReplica(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      String replicaId,
      String snapshotId)
      throws Exception {
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    replicaMetadataStore.createSync(
        new ReplicaMetadata(
            replicaId,
            snapshotId,
            "rep1",
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(60).toEpochMilli(),
            false));
  }

  private void initializeBlobStorageWithIndex(String snapshotId) throws Exception {
    LuceneIndexStoreImpl logStore =
        LuceneIndexStoreImpl.makeLogStore(
            Files.newTemporaryFolder(),
            Duration.ofSeconds(60),
            Duration.ofSeconds(60),
            true,
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_VALUE_AND_DUPLICATE_FIELD,
            meterRegistry);
    addMessages(logStore, 1, 10, true);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, meterRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, meterRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, meterRegistry)).isEqualTo(1);

    Path dirPath = logStore.getDirectory().getDirectory().toAbsolutePath();

    // Create schema file to upload
    ChunkSchema chunkSchema =
        new ChunkSchema(snapshotId, logStore.getSchema(), new ConcurrentHashMap<>());
    File schemaFile = new File(dirPath + "/" + SCHEMA_FILE_NAME);
    ChunkSchema.serializeToFile(chunkSchema, schemaFile);

    // Prepare list of files to upload.
    List<String> filesToUpload = new ArrayList<>();
    filesToUpload.add(schemaFile.getName());
    IndexCommit indexCommit = logStore.getIndexCommit();
    filesToUpload.addAll(indexCommit.getFileNames());

    assertThat(dirPath.toFile().listFiles().length).isGreaterThanOrEqualTo(filesToUpload.size());

    // Copy files to S3.
    blobStore.upload(snapshotId, dirPath);
  }

  private void initializeCacheNode(
      CacheNodeMetadataStore cacheNodeMetadataStore,
      String cacheNodeId,
      String hostname,
      long nodeCapacityBytes,
      String replicaSet,
      Boolean searchable)
      throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata(cacheNodeId, hostname, nodeCapacityBytes, replicaSet, searchable));
  }

  private void initializeCacheNodeAssignment(
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      String assignmentId,
      String snapshotId,
      String cacheNodeId,
      String replicaSet,
      String replicaId)
      throws Exception {
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            assignmentId,
            cacheNodeId,
            snapshotId,
            replicaId,
            replicaSet,
            0,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING));
  }

  private AstraConfigs.AstraConfig makeCacheConfig() {
    AstraConfigs.CacheConfig cacheConfig =
        AstraConfigs.CacheConfig.newBuilder()
            .setSlotsPerInstance(3)
            .setReplicaSet("rep1")
            .setDataDirectory(
                String.format(
                    "/tmp/%s/%s",
                    this.getClass().getSimpleName(), RandomStringUtils.randomAlphabetic(10)))
            .setServerConfig(
                AstraConfigs.ServerConfig.newBuilder()
                    .setServerAddress("localhost")
                    .setServerPort(8080)
                    .build())
            .build();

    AstraConfigs.S3Config s3Config =
        AstraConfigs.S3Config.newBuilder()
            .setS3Bucket(TEST_S3_BUCKET)
            .setS3Region("us-east-1")
            .build();

    return AstraConfigs.AstraConfig.newBuilder()
        .setCacheConfig(cacheConfig)
        .setS3Config(s3Config)
        .build();
  }
}
