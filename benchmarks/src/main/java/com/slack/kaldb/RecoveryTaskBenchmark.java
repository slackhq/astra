package com.slack.kaldb;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.recovery.RecoveryService;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.curator.test.TestingServer;
import org.junit.ClassRule;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import software.amazon.awssdk.services.s3.S3Client;

@State(Scope.Thread)
public class RecoveryTaskBenchmark {

  private static final String TEST_S3_BUCKET = "test-s3-bucket";

  @ClassRule
  private static final S3MockRule S3_MOCK_RULE =
      S3MockRule.builder().withInitialBuckets(TEST_S3_BUCKET).silent().build();

  private static final String TEST_KAFKA_TOPIC_1 = "test-topic-1";
  private static final String KALDB_TEST_CLIENT_1 = "kaldb-test-client1";

  private final Duration commitInterval = Duration.ofSeconds(5 * 60);
  private final Duration refreshInterval = Duration.ofSeconds(5 * 60);

  private Path tempDirectory;
  private MeterRegistry registry;
  LuceneIndexStoreImpl logStore;

  private TestingServer zkServer;
  private MeterRegistry meterRegistry;
  private BlobFs blobFs;
  public TestKafkaServer kafkaServer;
  private S3Client s3Client;
  private RecoveryService recoveryService;
  private ZookeeperMetadataStoreImpl metadataStore;

  @Setup(Level.Iteration)
  public void setUp(RecoveryTaskBenchmarkParams params) throws Exception {
    registry = new SimpleMeterRegistry();
    tempDirectory =
        Files.createDirectories(
            Paths.get("jmh-output", String.valueOf(new Random().nextInt(Integer.MAX_VALUE))));
    logStore =
        LuceneIndexStoreImpl.makeLogStore(
            tempDirectory.toFile(), commitInterval, refreshInterval, true, registry);

    kafkaServer = new TestKafkaServer();
    meterRegistry = new SimpleMeterRegistry();
    zkServer = new TestingServer();
    // s3MockRule = S3MockRule.builder().withInitialBuckets(TEST_S3_BUCKET).silent().build();
    s3Client = S3_MOCK_RULE.createS3ClientV2();
    blobFs = new S3BlobFs(s3Client);

    KaldbConfigs.KaldbConfig kaldbCfg = makeKaldbConfig(TEST_S3_BUCKET);
    metadataStore =
        ZookeeperMetadataStoreImpl.fromConfig(
            meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    // Start recovery service
    recoveryService = new RecoveryService(kaldbCfg, metadataStore, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Populate data in Kafka that will be used for recovery
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    produceMessagesToKafka(
        kafkaServer.getBroker(), startTime, TEST_KAFKA_TOPIC_1, 0, params.eventCount);
  }

  @TearDown(Level.Iteration)
  public void tearDown() throws Exception {
    logStore.close();
    try (Stream<Path> walk = Files.walk(tempDirectory)) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
    registry.close();

    if (recoveryService != null) {
      recoveryService.stopAsync();
      recoveryService.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (metadataStore != null) {
      metadataStore.close();
    }
    if (blobFs != null) {
      blobFs.close();
    }
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    if (zkServer != null) {
      zkServer.close();
    }
    if (meterRegistry != null) {
      meterRegistry.close();
    }
    if (s3Client != null) {
      s3Client.close();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void handleRecoveryTask(RecoveryTaskBenchmarkParams params) throws Exception {
    // Start recovery
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata(
            "testRecoveryTask", "0", 0, params.eventCount - 1, Instant.now().toEpochMilli());
    recoveryService.handleRecoveryTask(recoveryTask);
  }

  private KaldbConfigs.KaldbConfig makeKaldbConfig(String testS3Bucket) {
    return makeKaldbConfig(kafkaServer, testS3Bucket, TEST_KAFKA_TOPIC_1);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private KaldbConfigs.KaldbConfig makeKaldbConfig(
      TestKafkaServer testKafkaServer, String testS3Bucket, String topic) {
    return KaldbConfigUtil.makeKaldbConfig(
        "localhost:" + testKafkaServer.getBroker().getKafkaPort().get(),
        9000,
        topic,
        0,
        KALDB_TEST_CLIENT_1,
        testS3Bucket,
        9000 + 1,
        zkServer.getConnectString(),
        "recoveryZK_",
        KaldbConfigs.NodeRole.RECOVERY,
        10000,
        "api_log",
        9003,
        100);
  }

  @State(Scope.Benchmark)
  public static class RecoveryTaskBenchmarkParams {
    @Param({"1", "2", "4", "8", "16"})
    public int threadCount;

    @Param({"10000"})
    public int eventCount;
  }
}
