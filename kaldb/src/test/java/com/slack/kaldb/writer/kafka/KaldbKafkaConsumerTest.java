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
import static org.awaitility.Awaitility.with;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class KaldbKafkaConsumerTest {
  private static final String TEST_KAFKA_CLIENT_GROUP = "test_kaldb_consumer";

  public static class BasicTests {
    private static final String S3_TEST_BUCKET = "test-kaldb-logs";

    @ClassRule
    public static final S3MockRule S3_MOCK_RULE =
        S3MockRule.builder().withInitialBuckets(S3_TEST_BUCKET).silent().build();

    private TestKafkaServer kafkaServer;
    private KaldbKafkaConsumer testConsumer;
    private SimpleMeterRegistry metricsRegistry;
    private ChunkManagerUtil<LogMessage> chunkManagerUtil;

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Before
    public void setUp() throws Exception {
      kafkaServer = new TestKafkaServer();
      metricsRegistry = new SimpleMeterRegistry();

      chunkManagerUtil =
          makeChunkManagerUtil(
              S3_MOCK_RULE,
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
      testConsumer =
          new KaldbKafkaConsumer(
              TestKafkaServer.TEST_KAFKA_TOPIC,
              "0",
              kafkaServer.getBroker().getBrokerList().get(),
              TEST_KAFKA_CLIENT_GROUP,
              "true",
              "5000",
              "5000",
              logMessageWriter,
              metricsRegistry);
    }

    @After
    public void tearDown() throws Exception {
      chunkManagerUtil.close();
      testConsumer.close();
      kafkaServer.close();
      metricsRegistry.close();
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
      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

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
      final long startOffset = 101;
      TestKafkaComponents components =
          getTestKafkaServerWithConsumedOffsets(S3_MOCK_RULE, startOffset, 1300);

      Thread.sleep(30000);

      KaldbKafkaConsumer localConsumer2 =
          new KaldbKafkaConsumer(
              TestKafkaServer.TEST_KAFKA_TOPIC,
              "0",
              components.testKafkaServer.getBroker().getBrokerList().get(),
              TEST_KAFKA_CLIENT_GROUP,
              "true",
              "500",
              "500",
              components.logMessageWriter,
              components.meterRegistry,
              components.consumerOverrideProps);

      localConsumer2.prepConsumerForConsumption(startOffset);
      assertThatExceptionOfType(OffsetOutOfRangeException.class)
          .isThrownBy(
              () ->
                  localConsumer2.consumeMessagesBetweenOffsetsInParallel(
                      KAFKA_POLL_TIMEOUT_MS, startOffset, 1300));
    }

    public static TestKafkaComponents getTestKafkaServerWithConsumedOffsets(
        S3MockRule s3MockRule, long startOffset, long endOffset) throws Exception {
      Properties brokerOverrideProps = new Properties();
      brokerOverrideProps.put("log.retention.ms", "500");
      brokerOverrideProps.put("log.retention.check.interval.ms", "250");
      brokerOverrideProps.put("log.cleaner.backoff.ms", "250");
      brokerOverrideProps.put("log.segment.delete.delay.ms", "250");
      brokerOverrideProps.put("log.cleaner.enable", "true");
      // brokerOverrideProps.put("offsets.retention.minutes", "1");
      // brokerOverrideProps.put("log.retention.ms", "500");
      brokerOverrideProps.put("offsets.retention.check.interval.ms", "250");

      TestKafkaServer localKafkaServer = new TestKafkaServer(-1, brokerOverrideProps);
      SimpleMeterRegistry localMetricsRegistry = new SimpleMeterRegistry();

      AdminClient adminClient =
          AdminClient.create(
              Map.of(
                  AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                  localKafkaServer.getBroker().getBrokerList().get(),
                  AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                  "5000"));

      Properties consumerOverrideProps = new Properties();
      consumerOverrideProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "250");

      ChunkManagerUtil<LogMessage> localChunkManagerUtil =
          makeChunkManagerUtil(
              s3MockRule,
              localMetricsRegistry,
              10 * 1024 * 1024 * 1024L,
              10000L,
              KaldbConfigUtil.makeIndexerConfig());
      localChunkManagerUtil.chunkManager.startAsync();
      localChunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

      LogMessageWriterImpl logMessageWriter =
          new LogMessageWriterImpl(
              localChunkManagerUtil.chunkManager, LogMessageWriterImpl.apiLogTransformer);
      final KaldbKafkaConsumer localConsumer =
          new KaldbKafkaConsumer(
              TestKafkaServer.TEST_KAFKA_TOPIC,
              "0",
              localKafkaServer.getBroker().getBrokerList().get(),
              TEST_KAFKA_CLIENT_GROUP,
              "true",
              "500",
              "500",
              logMessageWriter,
              localMetricsRegistry,
              consumerOverrideProps);

      EphemeralKafkaBroker broker = localKafkaServer.getBroker();
      assertThat(broker.isRunning()).isTrue();
      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

      assertThat(localKafkaServer.getConnectedConsumerGroups()).isEqualTo(0);
      TestKafkaServer.produceMessagesToKafka(
          broker, startTime, TestKafkaServer.TEST_KAFKA_TOPIC, 0, 10000);
      await().until(() -> localConsumer.getEndOffSetForPartition() == 10000);

      TopicPartition topicPartition = new TopicPartition(TestKafkaServer.TEST_KAFKA_TOPIC, 0);
      ListOffsetsResult r = adminClient.listOffsets(Map.of(topicPartition, OffsetSpec.latest()));
      long endOffset1 = r.partitionResult(topicPartition).get().offset();
      // assertThat(startOffset1).isEqualTo(3);
      r = adminClient.listOffsets(Map.of(topicPartition, OffsetSpec.earliest()));
      long startOffset1 = r.partitionResult(topicPartition).get().offset();

      TestKafkaServer.produceMessagesToKafka(
          broker, startTime, TestKafkaServer.TEST_KAFKA_TOPIC, 0, 1);

      adminClient.alterConsumerGroupOffsets(
          TEST_KAFKA_CLIENT_GROUP, Map.of(topicPartition, new OffsetAndMetadata(5L)));

      r = adminClient.listOffsets(Map.of(topicPartition, OffsetSpec.latest()));
      long endOffset2 = r.partitionResult(topicPartition).get().offset();
      // assertThat(startOffset1).isEqualTo(3);
      r = adminClient.listOffsets(Map.of(topicPartition, OffsetSpec.earliest()));
      long startOffset2 = r.partitionResult(topicPartition).get().offset();

      long time1 = System.currentTimeMillis();
      with()
          .atMost(1, TimeUnit.MINUTES)
          .await()
          .until(() -> getStartOffset(adminClient, topicPartition) > 0);
      long time2 = System.currentTimeMillis();

      assertThat(time2 - time1).isLessThan(0);
      assertThat("1")
          .isEqualTo(startOffset1 + ", " + endOffset1 + ", " + startOffset2 + ", " + endOffset2);

      // consume specified offsets
      localConsumer.prepConsumerForConsumption(startOffset);
      localConsumer.consumeMessagesBetweenOffsetsInParallel(
          KAFKA_POLL_TIMEOUT_MS, startOffset, endOffset);
      // Check that messages are received and indexed.
      assertThat(getCount(RECORDS_RECEIVED_COUNTER, localMetricsRegistry))
          .isEqualTo(endOffset - startOffset + 1);
      assertThat(getValue(LIVE_MESSAGES_INDEXED, localMetricsRegistry))
          .isEqualTo(endOffset - startOffset + 1);
      assertThat(localKafkaServer.getConnectedConsumerGroups()).isEqualTo(0);
      localConsumer.close();
      return new TestKafkaComponents(
          localKafkaServer, logMessageWriter, localMetricsRegistry, consumerOverrideProps);
    }

    private static long getStartOffset(AdminClient adminClient, TopicPartition topicPartition)
        throws Exception {
      ListOffsetsResult r = adminClient.listOffsets(Map.of(topicPartition, OffsetSpec.earliest()));
      return r.partitionResult(topicPartition).get().offset();
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

  public static class TestKafkaComponents {
    public final TestKafkaServer testKafkaServer;
    public final LogMessageWriterImpl logMessageWriter;
    public final MeterRegistry meterRegistry;
    public final Properties consumerOverrideProps;

    public TestKafkaComponents(
        TestKafkaServer testKafkaServer,
        LogMessageWriterImpl logMessageWriter,
        MeterRegistry meterRegistry,
        Properties consumerOverrideProps) {
      this.testKafkaServer = testKafkaServer;
      this.logMessageWriter = logMessageWriter;
      this.meterRegistry = meterRegistry;
      this.consumerOverrideProps = consumerOverrideProps;
    }
  }

  public static class TimeoutTests {

    private static final String S3_TEST_BUCKET = "test-kaldb-logs";

    @ClassRule
    public static final S3MockRule S3_MOCK_RULE =
        S3MockRule.builder().withInitialBuckets(S3_TEST_BUCKET).silent().build();

    private TestKafkaServer kafkaServer;
    private KaldbKafkaConsumer testConsumer;
    private SimpleMeterRegistry metricsRegistry;
    private ChunkManagerUtil<LogMessage> chunkManagerUtil;
    private LogMessageWriterImpl logMessageWriter;

    @Before
    public void setUp() throws Exception {
      kafkaServer = new TestKafkaServer();
      metricsRegistry = new SimpleMeterRegistry();

      chunkManagerUtil =
          makeChunkManagerUtil(
              S3_MOCK_RULE,
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

    @After
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

      testConsumer =
          new KaldbKafkaConsumer(
              TestKafkaServer.TEST_KAFKA_TOPIC,
              "0",
              kafkaServer.getBroker().getBrokerList().get(),
              TEST_KAFKA_CLIENT_GROUP,
              "true",
              "5000",
              "5000",
              logMessageWriter,
              metricsRegistry);
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
}
