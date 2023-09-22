package com.slack.kaldb.writer.kafka;

import static com.slack.kaldb.chunkManager.RecoveryChunkManager.LIVE_MESSAGES_INDEXED;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.server.ValidateKaldbConfig.INDEXER_DATA_TRANSFORMER_MAP;
import static com.slack.kaldb.testlib.ChunkManagerUtil.makeChunkManagerUtil;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getValue;
import static com.slack.kaldb.writer.kafka.KaldbKafkaConsumer.KAFKA_POLL_TIMEOUT_MS;
import static com.slack.kaldb.writer.kafka.KaldbKafkaConsumer.RECORDS_RECEIVED_COUNTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.google.common.collect.Maps;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.schema.RaiseErrorFieldValueTest;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbKafkaConsumerTest {
  private static final Logger LOG = LoggerFactory.getLogger(RaiseErrorFieldValueTest.class);
  public static final String TEST_KAFKA_CLIENT_GROUP = "test_kaldb_consumer";
  private static final String S3_TEST_BUCKET = "test-kaldb-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withRootFolder(Files.newTemporaryFolder().toString())
          .withSecureConnection(false)
          .build();

  @Nested
  public class BasicTests {

    private TestKafkaServer kafkaServer;
    private KaldbKafkaConsumer testConsumer;
    private SimpleMeterRegistry metricsRegistry;
    private ChunkManagerUtil<LogMessage> chunkManagerUtil;

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @BeforeEach
    public void setUp() throws Exception {
      kafkaServer = new TestKafkaServer();
      metricsRegistry = new SimpleMeterRegistry();

      chunkManagerUtil =
          makeChunkManagerUtil(
              S3_MOCK_EXTENSION,
              S3_TEST_BUCKET,
              metricsRegistry,
              10 * 1024 * 1024 * 1024L,
              10000L,
              KaldbConfigUtil.makeIndexerConfig());
      chunkManagerUtil.chunkManager.startAsync();
      chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

      LogMessageWriterImpl logMessageWriter =
          new LogMessageWriterImpl(
              chunkManagerUtil.chunkManager, LogMessageWriterImpl.apiLogTransformer);
      KaldbConfigs.KafkaConfig kafkaConfig =
          KaldbConfigs.KafkaConfig.newBuilder()
              .setKafkaTopic(TestKafkaServer.TEST_KAFKA_TOPIC)
              .setKafkaTopicPartition("0")
              .setKafkaBootStrapServers(kafkaServer.getBroker().getBrokerList().get())
              .setKafkaClientGroup(TEST_KAFKA_CLIENT_GROUP)
              .setEnableKafkaAutoCommit("true")
              .setKafkaAutoCommitInterval("5000")
              .setKafkaSessionTimeout("5000")
              .build();

      testConsumer = new KaldbKafkaConsumer(kafkaConfig, logMessageWriter, metricsRegistry);
    }

    @AfterEach
    public void tearDown() throws Exception {
      chunkManagerUtil.close();
      testConsumer.close();
      kafkaServer.close();
      metricsRegistry.close();
    }

    @Test
    public void testOverridingProperties() {
      KaldbConfigs.KafkaConfig kafkaConfig =
          KaldbConfigs.KafkaConfig.newBuilder()
              .setKafkaTopic(TestKafkaServer.TEST_KAFKA_TOPIC)
              .setKafkaTopicPartition("0")
              .setKafkaBootStrapServers("bootstrap_server")
              .setKafkaClientGroup(TEST_KAFKA_CLIENT_GROUP)
              .setEnableKafkaAutoCommit("true")
              .setKafkaAutoCommitInterval("5000")
              .setKafkaSessionTimeout("5000")
              .build();

      Properties properties = KaldbKafkaConsumer.makeKafkaConsumerProps(kafkaConfig);
      assertThat(properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
          .isEqualTo("org.apache.kafka.common.serialization.StringDeserializer");

      kafkaConfig =
          KaldbConfigs.KafkaConfig.newBuilder()
              .setKafkaTopic(TestKafkaServer.TEST_KAFKA_TOPIC)
              .setKafkaTopicPartition("0")
              .setKafkaBootStrapServers("bootstrap_server")
              .setKafkaClientGroup(TEST_KAFKA_CLIENT_GROUP)
              .setEnableKafkaAutoCommit("true")
              .setKafkaAutoCommitInterval("5000")
              .setKafkaSessionTimeout("5000")
              .putAdditionalProps(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "test_serializer")
              .build();

      properties = KaldbKafkaConsumer.makeKafkaConsumerProps(kafkaConfig);
      assertThat(properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
          .isEqualTo("test_serializer");
    }

    @Test
    public void testGetEndOffsetForPartition() throws Exception {
      EphemeralKafkaBroker broker = kafkaServer.getBroker();
      assertThat(broker.isRunning()).isTrue();
      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

      assertThat(kafkaServer.getConnectedConsumerGroups()).isEqualTo(0);

      await().until(() -> testConsumer.getEndOffSetForPartition() == 0);
      TestKafkaServer.produceMessagesToKafka(broker, startTime);
      await().until(() -> testConsumer.getEndOffSetForPartition() == 100);
    }

    @Test
    public void testGetConsumerPositionForPartition() throws Exception {
      EphemeralKafkaBroker broker = kafkaServer.getBroker();
      assertThat(broker.isRunning()).isTrue();
      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

      assertThat(kafkaServer.getConnectedConsumerGroups()).isEqualTo(0);

      // Missing consumer throws an IllegalStateException.
      assertThatIllegalStateException()
          .isThrownBy(() -> testConsumer.getConsumerPositionForPartition());
      TestKafkaServer.produceMessagesToKafka(broker, startTime);
      await().until(() -> testConsumer.getEndOffSetForPartition() == 100);

      testConsumer.prepConsumerForConsumption(0);
      testConsumer.consumeMessages();
      assertThat(testConsumer.getConsumerPositionForPartition()).isEqualTo(100);
      assertThat(kafkaServer.getConnectedConsumerGroups()).isEqualTo(0);
      // Assign doesn't create a consumer group.
      assertThat(kafkaServer.getConnectedConsumerGroups()).isEqualTo(0);
    }

    @Test
    public void testConsumeMessagesBetweenOffsets() throws Exception {
      EphemeralKafkaBroker broker = kafkaServer.getBroker();
      assertThat(broker.isRunning()).isTrue();
      final Instant startTime = Instant.now();

      assertThat(kafkaServer.getConnectedConsumerGroups()).isEqualTo(0);

      // Missing consumer throws an IllegalStateException.
      assertThatIllegalStateException()
          .isThrownBy(() -> testConsumer.getConsumerPositionForPartition());
      // The kafka consumer fetches 500 messages per poll. So, generate lots of messages so we can
      // test the blocking logic of the consumer also.
      TestKafkaServer.produceMessagesToKafka(
          broker, startTime, TestKafkaServer.TEST_KAFKA_TOPIC, 0, 10000);
      await().until(() -> testConsumer.getEndOffSetForPartition() == 10000);

      final long startOffset = 101;
      testConsumer.prepConsumerForConsumption(startOffset);
      testConsumer.consumeMessagesBetweenOffsetsInParallel(
          KAFKA_POLL_TIMEOUT_MS, startOffset, 1300);
      // Check that messages are received and indexed.
      assertThat(getCount(RECORDS_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1200);
      assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(1200);
      // The consumer fetches 500 records per batch. So, the consumer offset is a bit ahead of
      // actual messages indexed.
      assertThat(testConsumer.getConsumerPositionForPartition()).isEqualTo(1601L);
      assertThat(kafkaServer.getConnectedConsumerGroups()).isEqualTo(0);
      // Assign doesn't create a consumer group.
      assertThat(kafkaServer.getConnectedConsumerGroups()).isEqualTo(0);
    }

    // TODO: Test batch ingestion with roll over. Not adding a test, since this functionality is
    // not needed by the recovery indexer yet.

    @Test
    public void testConsumptionOfUnavailableOffsetsThrowsException() throws Exception {
      final TopicPartition topicPartition = new TopicPartition(TestKafkaServer.TEST_KAFKA_TOPIC, 0);
      TestKafkaServer.KafkaComponents components = getKafkaTestServer(S3_MOCK_EXTENSION);

      KaldbConfigs.KafkaConfig kafkaConfig =
          KaldbConfigs.KafkaConfig.newBuilder()
              .setKafkaTopic(TestKafkaServer.TEST_KAFKA_TOPIC)
              .setKafkaTopicPartition("0")
              .setKafkaBootStrapServers(
                  components.testKafkaServer.getBroker().getBrokerList().get())
              .setKafkaClientGroup(TEST_KAFKA_CLIENT_GROUP)
              .setEnableKafkaAutoCommit("true")
              .setKafkaAutoCommitInterval("500")
              .setKafkaSessionTimeout("500")
              .putAllAdditionalProps(Maps.fromProperties(components.consumerOverrideProps))
              .build();

      final KaldbKafkaConsumer localTestConsumer =
          new KaldbKafkaConsumer(
              kafkaConfig, components.logMessageWriter, components.meterRegistry);

      // Missing consumer throws an IllegalStateException.
      assertThatIllegalStateException()
          .isThrownBy(() -> localTestConsumer.getConsumerPositionForPartition());

      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
      final long msgsToProduce = 100;
      TestKafkaServer.produceMessagesToKafka(
          components.testKafkaServer.getBroker(),
          startTime,
          topicPartition.topic(),
          topicPartition.partition(),
          (int) msgsToProduce);
      await().until(() -> localTestConsumer.getEndOffSetForPartition() == msgsToProduce);

      // we immediately force delete the messages, as this is faster than changing the retention and
      // waiting for the cleaner to run
      components
          .adminClient
          .deleteRecords(Map.of(topicPartition, RecordsToDelete.beforeOffset(100)))
          .all()
          .get();
      assertThat(getStartOffset(components.adminClient, topicPartition)).isGreaterThan(0);

      TestKafkaServer.produceMessagesToKafka(
          components.testKafkaServer.getBroker(),
          startTime,
          topicPartition.topic(),
          topicPartition.partition(),
          (int) msgsToProduce);
      await()
          .until(
              () -> localTestConsumer.getEndOffSetForPartition() == msgsToProduce + msgsToProduce);

      localTestConsumer.prepConsumerForConsumption(1);
      assertThatExceptionOfType(OffsetOutOfRangeException.class)
          .isThrownBy(
              () ->
                  localTestConsumer.consumeMessagesBetweenOffsetsInParallel(
                      KAFKA_POLL_TIMEOUT_MS, 0, msgsToProduce));
    }

    @Test
    public void testBlockingQueueDoesNotThrowException() {
      KaldbKafkaConsumer.BlockingArrayBlockingQueue<Object> q =
          new KaldbKafkaConsumer.BlockingArrayBlockingQueue<>(1);
      assertThat(q.offer(new Object())).isTrue();

      Thread t =
          new Thread(
              () -> {
                try {
                  Thread.sleep(1000);
                  q.take();
                } catch (InterruptedException e) {
                  // do nothing
                }
              });
      t.start();

      assertThat(q.offer(new Object())).isTrue();
    }
  }

  @Nested
  public class TimeoutTests {

    private static final String S3_TEST_BUCKET = "test-kaldb-logs";

    private TestKafkaServer kafkaServer;
    private KaldbKafkaConsumer testConsumer;
    private SimpleMeterRegistry metricsRegistry;
    private ChunkManagerUtil<LogMessage> chunkManagerUtil;
    private LogMessageWriterImpl logMessageWriter;

    @BeforeEach
    public void setUp() throws Exception {
      kafkaServer = new TestKafkaServer();
      metricsRegistry = new SimpleMeterRegistry();

      chunkManagerUtil =
          makeChunkManagerUtil(
              S3_MOCK_EXTENSION,
              S3_TEST_BUCKET,
              metricsRegistry,
              10 * 1024 * 1024 * 1024L,
              100L,
              KaldbConfigUtil.makeIndexerConfig());
      chunkManagerUtil.chunkManager.startAsync();
      chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

      logMessageWriter =
          new LogMessageWriterImpl(
              chunkManagerUtil.chunkManager, INDEXER_DATA_TRANSFORMER_MAP.get("spans"));
    }

    @AfterEach
    public void tearDown() throws Exception {
      chunkManagerUtil.close();
      if (testConsumer != null) {
        testConsumer.close();
      }
      kafkaServer.close();
      metricsRegistry.close();
    }

    @Test
    public void testThrowingConsumer() throws Exception {
      EphemeralKafkaBroker broker = kafkaServer.getBroker();
      assertThat(broker.isRunning()).isTrue();
      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
      assertThat(kafkaServer.getConnectedConsumerGroups()).isZero();

      TestKafkaServer.produceMessagesToKafka(broker, startTime);

      KaldbConfigs.KafkaConfig kafkaConfig =
          KaldbConfigs.KafkaConfig.newBuilder()
              .setKafkaTopic(TestKafkaServer.TEST_KAFKA_TOPIC)
              .setKafkaTopicPartition("0")
              .setKafkaBootStrapServers(kafkaServer.getBroker().getBrokerList().get())
              .setKafkaClientGroup(TEST_KAFKA_CLIENT_GROUP)
              .setEnableKafkaAutoCommit("true")
              .setKafkaAutoCommitInterval("5000")
              .setKafkaSessionTimeout("5000")
              .build();

      testConsumer = new KaldbKafkaConsumer(kafkaConfig, logMessageWriter, metricsRegistry);
      KafkaConsumer<String, byte[]> spyConsumer = spy(testConsumer.getKafkaConsumer());
      testConsumer.setKafkaConsumer(spyConsumer);
      await().until(() -> testConsumer.getEndOffSetForPartition() == 100);

      // Throw a run time exception when calling poll.
      doThrow(new RuntimeException("Runtime exception from test")).when(spyConsumer).poll(any());

      testConsumer.prepConsumerForConsumption(0);

      assertThatExceptionOfType(RuntimeException.class)
          .isThrownBy(() -> testConsumer.consumeMessages());

      testConsumer.close();
    }
  }

  public static void setRetentionTime(
      AdminClient adminClient, String topicName, int retentionTimeMs) {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    Collection<ConfigEntry> entries = new ArrayList<>();
    entries.add(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionTimeMs)));

    Config config = new Config(entries);
    Map<ConfigResource, Config> configs = new HashMap<>();
    configs.put(resource, config);

    adminClient.alterConfigs(configs);
  }

  public static long getStartOffset(AdminClient adminClient, TopicPartition topicPartition)
      throws Exception {
    ListOffsetsResult r = adminClient.listOffsets(Map.of(topicPartition, OffsetSpec.earliest()));
    return r.partitionResult(topicPartition).get().offset();
  }

  public static TestKafkaServer.KafkaComponents getKafkaTestServer(S3MockExtension s3MockExtension)
      throws Exception {
    Properties brokerOverrideProps = new Properties();
    brokerOverrideProps.put("log.retention.check.interval.ms", "250");
    brokerOverrideProps.put("log.cleaner.backoff.ms", "250");
    brokerOverrideProps.put("log.segment.delete.delay.ms", "250");
    brokerOverrideProps.put("log.cleaner.enable", "true");
    brokerOverrideProps.put("offsets.retention.check.interval.ms", "250");

    TestKafkaServer localKafkaServer = new TestKafkaServer(-1, brokerOverrideProps);
    SimpleMeterRegistry localMetricsRegistry = new SimpleMeterRegistry();

    EphemeralKafkaBroker broker = localKafkaServer.getBroker();
    assertThat(broker.isRunning()).isTrue();
    assertThat(localKafkaServer.getConnectedConsumerGroups()).isEqualTo(0);

    Properties consumerOverrideProps = new Properties();
    consumerOverrideProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "250");

    ChunkManagerUtil<LogMessage> localChunkManagerUtil =
        makeChunkManagerUtil(
            s3MockExtension,
            S3_TEST_BUCKET,
            localMetricsRegistry,
            10 * 1024 * 1024 * 1024L,
            10000L,
            KaldbConfigUtil.makeIndexerConfig());
    localChunkManagerUtil.chunkManager.startAsync();
    localChunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    LogMessageWriterImpl logMessageWriter =
        new LogMessageWriterImpl(
            localChunkManagerUtil.chunkManager, LogMessageWriterImpl.apiLogTransformer);

    AdminClient adminClient =
        AdminClient.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                localKafkaServer.getBroker().getBrokerList().get(),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                "5000"));

    return new TestKafkaServer.KafkaComponents(
        localKafkaServer,
        adminClient,
        logMessageWriter,
        localMetricsRegistry,
        consumerOverrideProps);
  }
}
