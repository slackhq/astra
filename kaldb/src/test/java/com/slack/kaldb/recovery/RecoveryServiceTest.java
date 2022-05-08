package com.slack.kaldb.recovery;

import static com.slack.kaldb.chunkManager.RollOverChunkTask.*;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.logstore.BlobFsUtils;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.junit.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class RecoveryServiceTest {

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static final String TEST_KAFKA_TOPIC_1 = "test-topic-1";
  private static final String KALDB_TEST_CLIENT_1 = "kaldb-test-client1";

  private TestingServer zkServer;
  private MeterRegistry meterRegistry;
  private BlobFs blobFs;
  private TestKafkaServer kafkaServer;
  private S3Client s3Client;

  @Before
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    kafkaServer = new TestKafkaServer();
    meterRegistry = new SimpleMeterRegistry();
    zkServer = new TestingServer();
    s3Client = S3_MOCK_RULE.createS3ClientV2();
    blobFs = new S3BlobFs(s3Client);
    s3Client.createBucket(CreateBucketRequest.builder().bucket(TEST_S3_BUCKET).build());
  }

  @After
  public void shutdown() throws Exception {
    blobFs.close();
    kafkaServer.close();
    zkServer.close();
    meterRegistry.close();
    s3Client.close();
  }

  @Ignore
  @Test
  public void shouldHandleRecoveryNodeLifecycle() throws Exception {
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("shouldHandleRecoveryNodeLifecycle")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    KaldbConfigs.ServerConfig serverConfig =
        KaldbConfigs.ServerConfig.newBuilder()
            .setServerPort(1234)
            .setServerAddress("localhost")
            .build();

    SearchContext searchContext = SearchContext.fromConfig(serverConfig);
    MetadataStore metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);

    RecoveryService recoveryService =
        new RecoveryService(
            KaldbConfigs.KaldbConfig.newBuilder().build(), metadataStore, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    RecoveryNodeMetadataStore recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(metadataStore, false);

    RecoveryNodeMetadata recoveryNodeMetadata =
        recoveryNodeMetadataStore.getNodeSync(searchContext.hostname);
    assertThat(recoveryNodeMetadata.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);

    RecoveryNodeMetadata recoveryNodeAssignment =
        new RecoveryNodeMetadata(
            recoveryNodeMetadata.name,
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
            "recoveryTaskName",
            Instant.now().toEpochMilli());
    recoveryNodeMetadataStore.updateSync(recoveryNodeAssignment);

    // todo - verify it actually indexes here before it returns back to free.
    //  Recovery task should not exist at this point if it is complete

    await()
        .until(
            () ->
                recoveryNodeMetadataStore.getNodeSync(searchContext.hostname).recoveryNodeState
                    == Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);

    recoveryService.stopAsync();
    recoveryService.awaitTerminated(DEFAULT_START_STOP_DURATION);

    recoveryNodeMetadataStore.close();

    metadataStore.close();
  }

  // TODO: change the params for recovery
  private KaldbConfigs.KaldbConfig makeKaldbConfig(
      int port,
      String kafkaTopic,
      int kafkaPartition,
      String clientName,
      String zkPathPrefix,
      KaldbConfigs.NodeRole nodeRole,
      int maxOffsetDelay,
      String testS3Bucket) {
    return KaldbConfigUtil.makeKaldbConfig(
        "localhost:" + kafkaServer.getBroker().getKafkaPort().get(),
        port,
        kafkaTopic,
        kafkaPartition,
        clientName,
        testS3Bucket,
        port + 1,
        zkServer.getConnectString(),
        zkPathPrefix,
        nodeRole,
        maxOffsetDelay,
        "api_log");
  }

  @Test
  public void testShouldHandleRecoveryTask() throws Exception {
    KaldbConfigs.KaldbConfig kaldbCfg =
        makeKaldbConfig(
            9000,
            TEST_KAFKA_TOPIC_1,
            0,
            KALDB_TEST_CLIENT_1,
            "recoveryZK_",
            KaldbConfigs.NodeRole.RECOVERY,
            10000,
            TEST_S3_BUCKET);
    MetadataStore metadataStore =
        ZookeeperMetadataStoreImpl.fromConfig(
            meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    // Start recovery service
    RecoveryService recoveryService =
        new RecoveryService(kaldbCfg, metadataStore, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Populate data in  Kafka so we can recover from it.
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final int indexedMessagesCount =
        produceMessagesToKafka(kafkaServer.getBroker(), startTime, TEST_KAFKA_TOPIC_1, 0);

    assertThat(blobFs.listFiles(BlobFsUtils.createURI(TEST_S3_BUCKET, "", ""), true)).isEmpty();
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    assertThat(snapshotMetadataStore.listSync().size()).isZero();
    // Start recovery
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata("testRecoveryTask", "0", 30, 60, Instant.now().toEpochMilli());
    assertThat(recoveryService.handleRecoveryTask(recoveryTask)).isTrue();
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(blobFs.listFiles(BlobFsUtils.createURI(TEST_S3_BUCKET, "/", ""), true)).isNotEmpty();
    assertThat(blobFs.exists(URI.create(snapshots.get(0).snapshotPath))).isTrue();
    assertThat(blobFs.listFiles(URI.create(snapshots.get(0).snapshotPath), false).length)
        .isGreaterThan(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, meterRegistry)).isEqualTo(31);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, meterRegistry)).isEqualTo(0);
  }

  // TODO: Chunk upload failure should fail the recovery task.
  // TODO: Add a multi chunk recovery task.

  // TODO: Add a test to test recovery task lifecycle for success and failure handle recovery task
  // cases.
}
