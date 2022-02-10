package com.slack.kaldb.writer.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.slack.kaldb.testlib.TestKafkaServer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KaldbKafkaConsumer2Test {
  private static final String TEST_KAFKA_CLIENT_GROUP = "test_kaldb_consumer";

  public static class BasicTests {
    private TestKafkaServer kafkaServer;
    private KaldbKafkaConsumer2 testConsumer;

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Before
    public void setUp() throws Exception {
      kafkaServer = new TestKafkaServer();
      testConsumer =
          new KaldbKafkaConsumer2(
              TestKafkaServer.TEST_KAFKA_TOPIC,
              "0",
              kafkaServer.getBroker().getBrokerList().get(),
              TEST_KAFKA_CLIENT_GROUP,
              "true",
              "5000",
              "5000");
    }

    @After
    public void tearDown() throws Exception {
      testConsumer.close();
      kafkaServer.close();
    }

    @Test
    public void testGetHeadOffsetForPartition() throws Exception {
      EphemeralKafkaBroker broker = kafkaServer.getBroker();
      assertThat(broker.isRunning()).isTrue();
      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

      assertThat(kafkaServer.getConnectedConsumerGroups()).isEqualTo(0);

      await().until(() -> testConsumer.getHeadOffSetForPartition() == 0);
      TestKafkaServer.produceMessagesToKafka(broker, startTime);
      await().until(() -> testConsumer.getHeadOffSetForPartition() == 100);
    }
  }
}
