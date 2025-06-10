package com.slack.astra.chunkManager;

import static com.slack.astra.chunk.ReadWriteChunk.SCHEMA_FILE_NAME;
import static com.slack.astra.chunkManager.CachingChunkManager.ASTRA_NG_DYNAMIC_CHUNK_SIZES_FLAG;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.MetricsUtil.getTimerCount;
import static com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension.addMessages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.chunk.Chunk;
import com.slack.astra.chunk.ReadOnlyChunkImpl;
import com.slack.astra.chunk.SearchContext;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LuceneIndexStoreImpl;
import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.schema.ChunkSchema;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.testlib.SpanUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

public class CachingChunkManagerTest {
  private static final String TEST_S3_BUCKET = "caching-chunkmanager-test";

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

  private AsyncCuratorFramework curatorFramework;
  private AstraConfigs.ZookeeperConfig zkConfig;
  private AstraConfigs.MetadataStoreConfig metadataStoreConfig;
  private CachingChunkManager<LogMessage> cachingChunkManager;
  private CacheNodeAssignmentStore cacheNodeAssignmentStore;
  private SnapshotMetadataStore snapshotMetadataStore;

  @BeforeEach
  public void startup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    S3AsyncClient s3AsyncClient =
        S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    blobStore = new BlobStore(s3AsyncClient, TEST_S3_BUCKET);
  }

  @AfterEach
  public void shutdown() throws IOException, TimeoutException {
    if (cachingChunkManager != null) {
      cachingChunkManager.stopAsync();
      cachingChunkManager.awaitTerminated(15, TimeUnit.SECONDS);
    }
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }
    testingServer.close();
    meterRegistry.close();
    disableDynamicChunksFlag();
  }

  private CachingChunkManager<LogMessage> initChunkManager() throws TimeoutException {
    AstraConfigs.CacheConfig cacheConfig =
        AstraConfigs.CacheConfig.newBuilder()
            .setSlotsPerInstance(3)
            .setReplicaSet("rep1")
            .setDataDirectory(
                String.format(
                    "/tmp/%s/%s", this.getClass().getSimpleName(), RandomStringUtils.random(10)))
            .setServerConfig(
                AstraConfigs.ServerConfig.newBuilder()
                    .setServerAddress("localhost")
                    .setServerPort(8080)
                    .build())
            .setCapacityBytes(4096)
            .build();

    AstraConfigs.S3Config s3Config =
        AstraConfigs.S3Config.newBuilder()
            .setS3Bucket(TEST_S3_BUCKET)
            .setS3Region("us-east-1")
            .build();

    AstraConfigs.AstraConfig AstraConfig =
        AstraConfigs.AstraConfig.newBuilder()
            .setCacheConfig(cacheConfig)
            .setS3Config(s3Config)
            .build();

    zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .setZkCacheInitTimeoutMs(1000)
            .build();

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);

    metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(zkConfig)
            .build();

    CachingChunkManager<LogMessage> cachingChunkManager =
        new CachingChunkManager<>(
            meterRegistry,
            curatorFramework,
            metadataStoreConfig,
            blobStore,
            SearchContext.fromConfig(AstraConfig.getCacheConfig().getServerConfig()),
            AstraConfig.getS3Config().getS3Bucket(),
            AstraConfig.getCacheConfig().getDataDirectory(),
            AstraConfig.getCacheConfig().getReplicaSet(),
            AstraConfig.getCacheConfig().getSlotsPerInstance(),
            AstraConfig.getCacheConfig().getCapacityBytes());

    cachingChunkManager.startAsync();
    cachingChunkManager.awaitRunning(15, TimeUnit.SECONDS);
    return cachingChunkManager;
  }

  private CacheNodeAssignment initAssignment(String snapshotId) throws Exception {
    cacheNodeAssignmentStore =
        new CacheNodeAssignmentStore(curatorFramework, metadataStoreConfig, meterRegistry);
    snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    snapshotMetadataStore.createSync(new SnapshotMetadata(snapshotId, 1, 1, 0, "abcd", 29));
    CacheNodeAssignment newAssignment =
        new CacheNodeAssignment(
            "abcd",
            cachingChunkManager.getId(),
            snapshotId,
            "replica1",
            "rep1",
            0,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING);
    cacheNodeAssignmentStore.createSync(newAssignment);
    return newAssignment;
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

    logStore.close();
    assertThat(dirPath.toFile().listFiles().length).isGreaterThanOrEqualTo(filesToUpload.size());

    // Copy files to S3.
    blobStore.upload(snapshotId, dirPath);
  }

  @Test
  public void shouldHandleLifecycle() throws Exception {
    cachingChunkManager = initChunkManager();

    assertThat(cachingChunkManager.getChunkList().size()).isEqualTo(3);

    List<Chunk<LogMessage>> readOnlyChunks = cachingChunkManager.getChunkList();
    await()
        .until(
            () ->
                ((ReadOnlyChunkImpl<?>) readOnlyChunks.get(0))
                    .getChunkMetadataState()
                    .equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE));
    await()
        .until(
            () ->
                ((ReadOnlyChunkImpl<?>) readOnlyChunks.get(1))
                    .getChunkMetadataState()
                    .equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE));
    await()
        .until(
            () ->
                ((ReadOnlyChunkImpl<?>) readOnlyChunks.get(2))
                    .getChunkMetadataState()
                    .equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE));
  }

  @Test
  public void testAddMessageIsUnsupported() throws TimeoutException {
    cachingChunkManager = initChunkManager();
    assertThatThrownBy(() -> cachingChunkManager.addMessage(SpanUtil.makeSpan(1), 10, "1", 1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testCreatesChunksOnAssignment() throws Exception {
    enableDynamicChunksFlag();
    String snapshotId = "abcd";

    cachingChunkManager = initChunkManager();
    initializeBlobStorageWithIndex(snapshotId);
    await()
        .ignoreExceptions()
        .until(
            () -> {
              Path path = Path.of("/tmp/test1");
              blobStore.download(snapshotId, path);
              return Objects.requireNonNull(path.toFile().listFiles()).length > 0;
            });
    initAssignment(snapshotId);

    await()
        .timeout(10000, TimeUnit.MILLISECONDS)
        .until(() -> cachingChunkManager.getChunksMap().size() == 1);
    assertThat(cachingChunkManager.getChunksMap().size()).isEqualTo(1);
  }

  @Test
  public void testChunkManagerRegistration() throws Exception {
    enableDynamicChunksFlag();

    cachingChunkManager = initChunkManager();
    CacheNodeMetadataStore cacheNodeMetadataStore =
        new CacheNodeMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);

    List<CacheNodeMetadata> cacheNodeMetadatas = cacheNodeMetadataStore.listSync();
    assertThat(cachingChunkManager.getChunkList().size()).isEqualTo(0);
    assertThat(cacheNodeMetadatas.size()).isEqualTo(1);
    assertThat(cacheNodeMetadatas.getFirst().nodeCapacityBytes).isEqualTo(4096);
    assertThat(cacheNodeMetadatas.getFirst().replicaSet).isEqualTo("rep1");
    assertThat(cacheNodeMetadatas.getFirst().id).isEqualTo(cachingChunkManager.getId());

    cacheNodeMetadataStore.close();
  }

  @Test
  public void testBasicChunkEviction() throws Exception {
    enableDynamicChunksFlag();
    String snapshotId = "abcd";

    cachingChunkManager = initChunkManager();
    initializeBlobStorageWithIndex(snapshotId);
    await()
        .ignoreExceptions()
        .until(
            () -> {
              Path path = Path.of("/tmp/test2");
              blobStore.download(snapshotId, path);
              return Objects.requireNonNull(path.toFile().listFiles()).length > 0;
            });

    CacheNodeAssignment assignment = initAssignment(snapshotId);

    // assert chunks created
    await()
        .timeout(10000, TimeUnit.MILLISECONDS)
        .until(() -> cachingChunkManager.getChunksMap().size() == 1);
    assertThat(cachingChunkManager.getChunksMap().size()).isEqualTo(1);

    cacheNodeAssignmentStore.updateAssignmentState(
        assignment, Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT);

    await()
        .timeout(10000, TimeUnit.MILLISECONDS)
        .until(() -> cachingChunkManager.getChunksMap().isEmpty());
    assertThat(cacheNodeAssignmentStore.listSync().size()).isEqualTo(0);
  }

  private static void enableDynamicChunksFlag() {
    System.setProperty(ASTRA_NG_DYNAMIC_CHUNK_SIZES_FLAG, "true");
  }

  private static void disableDynamicChunksFlag() {
    System.setProperty(ASTRA_NG_DYNAMIC_CHUNK_SIZES_FLAG, "false");
  }

  // TODO: Add a unit test to ensure caching chunk manager can search messages.
  // TODO: Add a unit test to ensure that all chunks in caching chunk manager are read only.
  // TODO: Add a unit test to ensure that caching chunk manager can handle exceptions gracefully.
}
