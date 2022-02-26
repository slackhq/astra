package com.slack.kaldb.writer.kafka;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.config.KaldbConfig.dataTransformerMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbKafkaConsumer2Test {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbKafkaConsumer2Test.class);

  private static final String TEST_KAFKA_CLIENT_GROUP = "test_kaldb_consumer";

  public static class BasicTests {
    @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

    private TestKafkaServer kafkaServer;
    private KaldbKafkaConsumer2 testConsumer;
    private SimpleMeterRegistry metricsRegistry;
    private ChunkManagerUtil<LogMessage> chunkManagerUtil;
    private LogMessageWriterImpl logMessageWriter;

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Before
    public void setUp() throws Exception {
      kafkaServer = new TestKafkaServer();
      metricsRegistry = new SimpleMeterRegistry();

      chunkManagerUtil =
          new ChunkManagerUtil<>(S3_MOCK_RULE, metricsRegistry, 10 * 1024 * 1024 * 1024L, 100);
      chunkManagerUtil.chunkManager.startAsync();
      chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

      logMessageWriter =
          new LogMessageWriterImpl(chunkManagerUtil.chunkManager, dataTransformerMap.get("spans"));
      testConsumer =
          new KaldbKafkaConsumer2(
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
  }

  public static class TimeoutTests {

    @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

    private TestKafkaServer kafkaServer;
    private KaldbKafkaConsumer2 testConsumer;
    private SimpleMeterRegistry metricsRegistry;
    private ChunkManagerUtil<LogMessage> chunkManagerUtil;
    private LogMessageWriterImpl logMessageWriter;

    @Before
    public void setUp() throws Exception {
      kafkaServer = new TestKafkaServer();
      metricsRegistry = new SimpleMeterRegistry();

      chunkManagerUtil =
          new ChunkManagerUtil<>(S3_MOCK_RULE, metricsRegistry, 10 * 1024 * 1024 * 1024L, 100);
      chunkManagerUtil.chunkManager.startAsync();
      chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

      logMessageWriter =
          new LogMessageWriterImpl(chunkManagerUtil.chunkManager, dataTransformerMap.get("spans"));
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
          new KaldbKafkaConsumer2(
              TestKafkaServer.TEST_KAFKA_TOPIC,
              "0",
              kafkaServer.getBroker().getBrokerList().get(),
              TEST_KAFKA_CLIENT_GROUP,
              "true",
              "5000",
              "5000",
              logMessageWriter,
              metricsRegistry);
      KafkaConsumer<String, byte[]> consumer = spy(testConsumer.getKafkaConsumer());
      await().until(() -> testConsumer.getEndOffSetForPartition() == 100);

      // Throw a run time exception when calling poll.
      doThrow(new RuntimeException("Runtime exception from test")).when(consumer).poll(any());

      testConsumer.prepConsumerForConsumption(0);
      // TODO: Throw an exception?
      //      await()
      //          .until(
      //              () -> {
      //                testConsumer.consumeMessages();
      //                double recordCount = getCount(RECORDS_RECEIVED_COUNTER, metricsRegistry);
      //                if (recordCount != 100) {
      //                  LOG.warn(String.format("Current record count %s", recordCount));
      //                }
      //                return recordCount == 100;
      //              });

      testConsumer.close();
    }
  }
}
