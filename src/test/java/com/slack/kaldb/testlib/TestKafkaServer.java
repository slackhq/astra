package com.slack.kaldb.testlib;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.util.JsonUtil;
import com.slack.service.murron.Murron;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestKafkaServer {
  public static final String TEST_KAFKA_TOPIC = "test-topic";
  // Kafka producer creates only a partition 0 on first message. So, set the partition to 0 always.
  public static final int TEST_KAFKA_PARTITION = 0;

  // Create messages, format them into murron protobufs, write them to kafka
  public static void produceMessagesToKafka(
      EphemeralKafkaBroker broker, Instant startTime, String kafkaTopic, int partitionNumber)
      throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException {
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    // Insert messages into Kafka.
    try (KafkaProducer<String, byte[]> producer =
        broker.createProducer(new StringSerializer(), new ByteArraySerializer(), null)) {
      int i = 0;
      for (LogMessage msg : messages) {

        Future<RecordMetadata> result =
            producer.send(
                new ProducerRecord<>(
                    kafkaTopic,
                    partitionNumber,
                    String.valueOf(i),
                    fromLogMessage(msg, i).toByteArray()));

        RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(kafkaTopic);
        i++;
      }
    }
  }

  public static void produceMessagesToKafka(EphemeralKafkaBroker broker, Instant startTime)
      throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException {
    produceMessagesToKafka(broker, startTime, TEST_KAFKA_TOPIC, TEST_KAFKA_PARTITION);
  }

  public static Murron.MurronMessage fromLogMessage(LogMessage message, int offset)
      throws JsonProcessingException {
    String jsonStr = JsonUtil.writeAsString(message.source);
    return Murron.MurronMessage.newBuilder()
        .setTimestamp(message.timeSinceEpochMilli * 1000 * 1000)
        .setType(MessageUtil.TEST_INDEX_NAME)
        .setHost("localhost")
        .setPid(100)
        .setOffset(offset)
        .setMessage(ByteString.copyFromUtf8(jsonStr))
        .build();
  }

  private final EphemeralKafkaBroker broker;
  private final CompletableFuture<Void> brokerStart;
  private final AdminClient adminClient;
  private Path logDir;

  public TestKafkaServer() throws Exception {
    // Create a kafka broker
    broker = EphemeralKafkaBroker.create();
    brokerStart = broker.start();
    Futures.getUnchecked(brokerStart);

    logDir = Paths.get(broker.getLogDir().get());
    assertThat(Files.exists(logDir)).isTrue();

    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokerList().get());
    adminClient = AdminClient.create(props);
  }

  public int getConnectedConsumerGroups() throws ExecutionException, InterruptedException {
    return adminClient.listConsumerGroups().all().get().size();
  }

  public CompletableFuture<Void> getBrokerStart() {
    return brokerStart;
  }

  public EphemeralKafkaBroker getBroker() {
    return broker;
  }

  public void close() throws ExecutionException, InterruptedException {
    if (broker != null) {
      broker.stop();
    }
    assertThat(brokerStart.isDone()).isTrue();
    assertThat(broker.isRunning()).isFalse();
    assertThat(broker.getBrokerList().isPresent()).isFalse();
    assertThat(broker.getZookeeperConnectString().isPresent()).isFalse();
    assertThat(Files.exists(logDir)).isFalse();
  }
}
