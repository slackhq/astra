package com.slack.astra.testlib;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.chunk.SearchContext;
import com.slack.astra.chunkManager.IndexingChunkManager;
import com.slack.astra.chunkrollover.ChunkRollOverStrategy;
import com.slack.astra.chunkrollover.DiskOrMessageCountBasedRolloverStrategy;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * This class creates a chunk manager that can be used in unit tests.
 *
 * <p>TODO: Convert this class into a test rule to make it easy to use.
 */
public class ChunkManagerUtil<T> {

  public static final String TEST_HOST = "localhost";
  public static final int TEST_PORT = 34567;

  private final File tempFolder;
  public S3AsyncClient s3AsyncClient;
  public static final String METADATA_PATH_PREFIX = "testMetadata";
  public final IndexingChunkManager<T> chunkManager;
  private final TestingServer zkServer;
  private final AsyncCuratorFramework curatorFramework;
  private final Client etcdClient;
  private final boolean autoCloseResources;

  @Deprecated // please use the non-static version
  public static ChunkManagerUtil<LogMessage> makeChunkManagerUtil(
      S3MockExtension s3MockExtension,
      String s3Bucket,
      MeterRegistry meterRegistry,
      long maxBytesPerChunk,
      long maxMessagesPerChunk,
      AstraConfigs.IndexerConfig indexerConfig)
      throws Exception {

    TestingServer zkServer = new TestingServer();
    EtcdCluster etcdCluster = TestEtcdClusterFactory.start();

    // Create etcd client
    Client etcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .namespace(
                ByteSequence.from(METADATA_PATH_PREFIX, java.nio.charset.StandardCharsets.UTF_8))
            .build();

    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace(METADATA_PATH_PREFIX)
            .setEnabled(true)
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
            .build();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .putStoreModes("DatasetMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SnapshotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("ReplicaMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("HpaMetricMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SearchMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheSlotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeAssignmentStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes(
                "FieldRedactionMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("PreprocessorMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryTaskMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(zkServer.getConnectString())
                    .setZkPathPrefix(METADATA_PATH_PREFIX)
                    .setZkSessionTimeoutMs(30000)
                    .setZkConnectionTimeoutMs(30000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .setEtcdConfig(etcdConfig)
            .build();
    AsyncCuratorFramework curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());

    return new ChunkManagerUtil<>(
        s3MockExtension,
        s3Bucket,
        meterRegistry,
        zkServer,
        maxBytesPerChunk,
        maxMessagesPerChunk,
        new SearchContext(TEST_HOST, TEST_PORT),
        curatorFramework,
        indexerConfig,
        metadataStoreConfig,
        etcdCluster,
        etcdClient,
        true);
  }

  public ChunkManagerUtil(
      S3MockExtension s3MockExtension,
      String s3Bucket,
      MeterRegistry meterRegistry,
      TestingServer zkServer,
      long maxBytesPerChunk,
      long maxMessagesPerChunk,
      SearchContext searchContext,
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.IndexerConfig indexerConfig,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      EtcdCluster etcdCluster,
      Client etcdClient,
      boolean autoCloseResources)
      throws Exception {

    this.autoCloseResources = autoCloseResources;

    tempFolder =
        Files.createTempDir(); // TODO: replace with java.nio.file.Files.createTempDirectory
    s3AsyncClient = S3TestUtils.createS3CrtClient(s3MockExtension.getServiceEndpoint());
    BlobStore blobStore = new BlobStore(s3AsyncClient, s3Bucket);

    this.zkServer = zkServer;
    // noop if zk has already been started by the caller
    this.zkServer.start();

    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(
            meterRegistry, maxBytesPerChunk, maxMessagesPerChunk);

    this.curatorFramework = curatorFramework;
    this.etcdClient = etcdClient;

    chunkManager =
        new IndexingChunkManager<>(
            "testData",
            tempFolder.getAbsolutePath(),
            chunkRollOverStrategy,
            meterRegistry,
            blobStore,
            MoreExecutors.newDirectExecutorService(),
            curatorFramework,
            etcdClient,
            searchContext,
            indexerConfig,
            AstraConfigs.LuceneConfig.newBuilder()
                .setCommitDurationSecs(10)
                .setRefreshDurationSecs(10)
                .setEnableFullTextSearch(true)
                .build(),
            metadataStoreConfig);
  }

  public void close() throws IOException, TimeoutException {
    chunkManager.stopAsync();
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    s3AsyncClient.close();

    if (autoCloseResources) {
      // This is a hack around the fact we create closable resources in a static method
      // and then lose references to them. Th
      curatorFramework.unwrap().close();
      zkServer.close();
      if (etcdClient != null) {
        etcdClient.close();
      }
    }

    FileUtils.deleteDirectory(tempFolder);
  }

  public static List<SnapshotMetadata> fetchNonLiveSnapshot(List<SnapshotMetadata> snapshots) {
    Predicate<SnapshotMetadata> nonLiveSnapshotPredicate = s -> !s.isLive();
    return fetchSnapshotMatching(snapshots, nonLiveSnapshotPredicate);
  }

  public static List<SnapshotMetadata> fetchLiveSnapshot(List<SnapshotMetadata> snapshots) {
    Predicate<SnapshotMetadata> liveSnapshotPredicate = SnapshotMetadata::isLive;
    return fetchSnapshotMatching(snapshots, liveSnapshotPredicate);
  }

  public static List<SnapshotMetadata> fetchSnapshotMatching(
      List<SnapshotMetadata> afterSnapshots, Predicate<SnapshotMetadata> condition) {
    return afterSnapshots.stream().filter(condition).collect(Collectors.toList());
  }

  public AsyncCuratorFramework getCuratorFramework() {
    return curatorFramework;
  }
}
