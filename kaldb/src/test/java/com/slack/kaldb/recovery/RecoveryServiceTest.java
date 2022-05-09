package com.slack.kaldb.recovery;

import static com.slack.kaldb.chunkManager.RollOverChunkTask.*;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.recovery.RecoveryService.*;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.logstore.BlobFsUtils;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
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
  private RecoveryService recoveryService;
  private ZookeeperMetadataStoreImpl metadataStore;

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
    if (recoveryService != null) {
      recoveryService.stopAsync();
      recoveryService.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (metadataStore != null) {
      metadataStore.close();
    }
    blobFs.close();
    kafkaServer.close();
    zkServer.close();
    meterRegistry.close();
    s3Client.close();
  }

  // TODO: change the params for recovery
  private KaldbConfigs.KaldbConfig makeKaldbConfig(String testS3Bucket) {
    return KaldbConfigUtil.makeKaldbConfig(
        "localhost:" + kafkaServer.getBroker().getKafkaPort().get(),
        9000,
        RecoveryServiceTest.TEST_KAFKA_TOPIC_1,
        0,
        RecoveryServiceTest.KALDB_TEST_CLIENT_1,
        testS3Bucket,
        9000 + 1,
        zkServer.getConnectString(),
        "recoveryZK_",
        KaldbConfigs.NodeRole.RECOVERY,
        10000,
        "api_log");
  }

  @Test
  public void testShouldHandleRecoveryTask() throws Exception {
    KaldbConfigs.KaldbConfig kaldbCfg = makeKaldbConfig(TEST_S3_BUCKET);
    metadataStore =
        ZookeeperMetadataStoreImpl.fromConfig(
            meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    // Start recovery service
    recoveryService = new RecoveryService(kaldbCfg, metadataStore, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Populate data in  Kafka so we can recover from it.
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    produceMessagesToKafka(kafkaServer.getBroker(), startTime, TEST_KAFKA_TOPIC_1, 0);

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

  @Test
  public void testShouldHandleRecoveryTaskFailure() throws Exception {
    String fakeS3Bucket = "fakeBucket";
    KaldbConfigs.KaldbConfig kaldbCfg = makeKaldbConfig(fakeS3Bucket);
    metadataStore =
        ZookeeperMetadataStoreImpl.fromConfig(
            meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    // Start recovery service
    recoveryService = new RecoveryService(kaldbCfg, metadataStore, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Populate data in  Kafka so we can recover from it.
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    produceMessagesToKafka(kafkaServer.getBroker(), startTime, TEST_KAFKA_TOPIC_1, 0);

    assertThat(s3Client.listBuckets().buckets().size()).isEqualTo(1);
    assertThat(s3Client.listBuckets().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);
    assertThat(s3Client.listBuckets().buckets().get(0).name()).isNotEqualTo(fakeS3Bucket);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    assertThat(snapshotMetadataStore.listSync().size()).isZero();

    // Start recovery
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata("testRecoveryTask", "0", 30, 60, Instant.now().toEpochMilli());
    assertThat(recoveryService.handleRecoveryTask(recoveryTask)).isFalse();

    assertThat(s3Client.listBuckets().buckets().size()).isEqualTo(1);
    assertThat(s3Client.listBuckets().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);
    assertThat(s3Client.listBuckets().buckets().get(0).name()).isNotEqualTo(fakeS3Bucket);

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, meterRegistry)).isEqualTo(31);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_FAILED, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void testShouldHandleRecoveryTaskAssignmentSuccess() throws Exception {
    KaldbConfigs.KaldbConfig kaldbCfg = makeKaldbConfig(TEST_S3_BUCKET);
    metadataStore =
        ZookeeperMetadataStoreImpl.fromConfig(
            meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    // Start recovery service
    recoveryService = new RecoveryService(kaldbCfg, metadataStore, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Populate data in  Kafka so we can recover data from Kafka.
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    produceMessagesToKafka(kafkaServer.getBroker(), startTime, TEST_KAFKA_TOPIC_1, 0);

    assertThat(s3Client.listBuckets().buckets().size()).isEqualTo(1);
    assertThat(s3Client.listBuckets().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    assertThat(snapshotMetadataStore.listSync().size()).isZero();

    assertThat(snapshotMetadataStore.listSync().size()).isZero();
    // Create a recovery task
    RecoveryTaskMetadataStore recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(metadataStore, false);
    assertThat(recoveryTaskMetadataStore.listSync().size()).isZero();
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata("testRecoveryTask", "0", 30, 60, Instant.now().toEpochMilli());
    recoveryTaskMetadataStore.createSync(recoveryTask);
    assertThat(recoveryTaskMetadataStore.listSync().size()).isEqualTo(1);
    assertThat(recoveryTaskMetadataStore.listSync().get(0)).isEqualTo(recoveryTask);

    // Assign the recovery task to node.
    RecoveryNodeMetadataStore recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(metadataStore, false);
    List<RecoveryNodeMetadata> recoveryNodes = recoveryNodeMetadataStore.listSync();
    assertThat(recoveryNodes.size()).isEqualTo(1);
    RecoveryNodeMetadata recoveryNodeMetadata = recoveryNodes.get(0);
    assertThat(recoveryNodeMetadata.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
    recoveryNodeMetadataStore.updateSync(
        new RecoveryNodeMetadata(
            recoveryNodeMetadata.getName(),
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
            recoveryTask.getName(),
            Instant.now().toEpochMilli()));
    assertThat(recoveryTaskMetadataStore.listSync().size()).isEqualTo(1);

    await().until(() -> getCount(RECOVERY_NODE_ASSIGNMENT_SUCCESS, meterRegistry) == 1);
    assertThat(getCount(RECOVERY_NODE_ASSIGNMENT_RECEIVED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(RECOVERY_NODE_ASSIGNMENT_FAILED, meterRegistry)).isZero();

    // Check metadata
    assertThat(s3Client.listBuckets().buckets().size()).isEqualTo(1);
    assertThat(s3Client.listBuckets().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);

    // Post recovery checks
    assertThat(recoveryNodeMetadataStore.listSync().size()).isEqualTo(1);
    assertThat(recoveryNodeMetadataStore.listSync().get(0).recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);

    // 1 snapshot is published
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(1);
    assertThat(blobFs.exists(URI.create(snapshots.get(0).snapshotPath))).isTrue();
    assertThat(blobFs.listFiles(URI.create(snapshots.get(0).snapshotPath), false).length)
        .isGreaterThan(1);

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, meterRegistry)).isEqualTo(31);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testShouldHandleRecoveryTaskAssignmentFailure() throws Exception {
    String fakeS3Bucket = "fakeS3Bucket";
    KaldbConfigs.KaldbConfig kaldbCfg = makeKaldbConfig(fakeS3Bucket);
    metadataStore =
        ZookeeperMetadataStoreImpl.fromConfig(
            meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    // Start recovery service
    recoveryService = new RecoveryService(kaldbCfg, metadataStore, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Populate data in  Kafka so we can recover data from Kafka.
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    produceMessagesToKafka(kafkaServer.getBroker(), startTime, TEST_KAFKA_TOPIC_1, 0);

    // fakeS3Bucket is not present.
    assertThat(s3Client.listBuckets().buckets().size()).isEqualTo(1);
    assertThat(s3Client.listBuckets().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    assertThat(snapshotMetadataStore.listSync().size()).isZero();

    assertThat(snapshotMetadataStore.listSync().size()).isZero();
    // Create a recovery task
    RecoveryTaskMetadataStore recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(metadataStore, false);
    assertThat(recoveryTaskMetadataStore.listSync().size()).isZero();
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata("testRecoveryTask", "0", 30, 60, Instant.now().toEpochMilli());
    recoveryTaskMetadataStore.createSync(recoveryTask);
    assertThat(recoveryTaskMetadataStore.listSync().size()).isEqualTo(1);
    assertThat(recoveryTaskMetadataStore.listSync().get(0)).isEqualTo(recoveryTask);

    // Assign the recovery task to node.
    RecoveryNodeMetadataStore recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(metadataStore, false);
    List<RecoveryNodeMetadata> recoveryNodes = recoveryNodeMetadataStore.listSync();
    assertThat(recoveryNodes.size()).isEqualTo(1);
    RecoveryNodeMetadata recoveryNodeMetadata = recoveryNodes.get(0);
    assertThat(recoveryNodeMetadata.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
    recoveryNodeMetadataStore.updateSync(
        new RecoveryNodeMetadata(
            recoveryNodeMetadata.getName(),
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
            recoveryTask.getName(),
            Instant.now().toEpochMilli()));
    assertThat(recoveryTaskMetadataStore.listSync().size()).isEqualTo(1);

    await().until(() -> getCount(RECOVERY_NODE_ASSIGNMENT_FAILED, meterRegistry) == 1);
    assertThat(getCount(RECOVERY_NODE_ASSIGNMENT_RECEIVED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(RECOVERY_NODE_ASSIGNMENT_SUCCESS, meterRegistry)).isZero();

    // Check metadata
    assertThat(s3Client.listBuckets().buckets().size()).isEqualTo(1);
    assertThat(s3Client.listBuckets().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);

    // Post recovery checks
    assertThat(recoveryNodeMetadataStore.listSync().size()).isEqualTo(1);
    assertThat(recoveryNodeMetadataStore.listSync().get(0).recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);

    // Recovery task still exists for re-assignment.
    assertThat(recoveryTaskMetadataStore.listSync().size()).isEqualTo(1);
    assertThat(recoveryTaskMetadataStore.listSync().get(0)).isEqualTo(recoveryTask);

    // No snapshots are published on failure.
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    assertThat(snapshotMetadataStore.listSync().size()).isZero();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, meterRegistry)).isEqualTo(31);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_FAILED, meterRegistry)).isEqualTo(1);
  }

  // TODO: Add a multi chunk recovery task.
}
