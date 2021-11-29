package com.slack.kaldb.writer.kafka;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.with;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.slack.kaldb.testlib.TestKafkaServer;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Enclosed.class)
public class KaldbKafkaConsumerTest {
  // TODO: Add a test to make sure catchup is working as expected.
  private static final Logger LOG = LoggerFactory.getLogger(KaldbKafkaConsumerTest.class);

  static class TestKaldbKafkaConsumer extends KaldbKafkaConsumer {
    private final AtomicInteger recordCount = new AtomicInteger(0);

    public TestKaldbKafkaConsumer(String kafkaBootStrapServers, String testKafkaClientGroup) {
      super(
          TestKafkaServer.TEST_KAFKA_TOPIC,
          "0",
          kafkaBootStrapServers,
          testKafkaClientGroup,
          "true",
          "5000",
          "5000");
    }

    @Override
    void consumeMessages(long kafkaPollTimeoutMs) {
      ConsumerRecords<String, byte[]> records =
          getConsumer().poll(Duration.ofMillis(kafkaPollTimeoutMs));
      recordCount.addAndGet(records.count());
    }

    public int getRecordCount() {
      return recordCount.get();
    }
  }

  public static class BasicTests {
    private TestKafkaServer kafkaServer;
    private TestKaldbKafkaConsumer testConsumer;

    @Before
    public void setUp() throws Exception {
      kafkaServer = new TestKafkaServer();
      testConsumer =
          new TestKaldbKafkaConsumer(kafkaServer.getBroker().getBrokerList().get(), "basic_tests");
    }

    @After
    public void tearDown()
        throws ExecutionException, InterruptedException, TimeoutException, NoSuchFieldException,
            IllegalAccessException, IOException {
      testConsumer.stopAsync();
      testConsumer.awaitTerminated(DEFAULT_START_STOP_DURATION);
      assertThat(testConsumer.isRunning()).isFalse();

      // Close server after consumer done.
      kafkaServer.close();
    }

    @Test
    public void kafkaConsumerStartupShutdown() throws Exception {
      EphemeralKafkaBroker broker = kafkaServer.getBroker();
      assertThat(broker.isRunning()).isTrue();
      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

      testConsumer.startAsync();
      testConsumer.awaitRunning(DEFAULT_START_STOP_DURATION);
      await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

      TestKafkaServer.produceMessagesToKafka(broker, startTime);
      await().until(() -> testConsumer.getRecordCount() == 100);
    }
  }

  public static class TimeoutTests {
    private TestKafkaServer kafkaServer;
    private TestKaldbKafkaConsumer testConsumer;

    @Before
    public void setUp() throws Exception {
      kafkaServer = new TestKafkaServer();
      testConsumer =
          new TestKaldbKafkaConsumer(
              kafkaServer.getBroker().getBrokerList().get(), "timeout_tests");
    }

    @After
    public void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
      // KafkaConsumer.DEFAULT_CLOSE_TIMEOUT_MS is 30 * 1000, so wait that plus a little extra
      with()
          .pollInterval(1, TimeUnit.SECONDS)
          .await()
          .atMost(45, TimeUnit.SECONDS)
          .until(() -> !testConsumer.isRunning());
    }

    @Test(expected = TimeoutException.class)
    public void kafkaConsumerShutdownTimeout() throws Exception {
      EphemeralKafkaBroker broker = kafkaServer.getBroker();
      assertThat(broker.isRunning()).isTrue();
      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

      testConsumer.startAsync();
      testConsumer.awaitRunning(DEFAULT_START_STOP_DURATION);
      await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

      TestKafkaServer.produceMessagesToKafka(broker, startTime);
      await()
          .until(
              () -> {
                long recordCount = testConsumer.getRecordCount();
                if (recordCount != 100) {
                  LOG.warn(String.format("Current record count %s", recordCount));
                }
                return recordCount == 100;
              });

      // Closing server before consumer should lead to time out.
      kafkaServer.close();
      testConsumer.stopAsync();
      testConsumer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
  }
}
