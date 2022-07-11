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
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestKafkaServer {
  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaServer.class);
  private static final int ALLOCATE_RANDOM_PORT = -1;

  public static final String TEST_KAFKA_TOPIC = "test-topic";

  // Create messages, format them into murron protobufs, write them to kafka
  public static int produceMessagesToKafka(
      EphemeralKafkaBroker broker, Instant startTime, String kafkaTopic, int partitionId, int count)
      throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException {
    List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, count, 1000, startTime);

    int indexedCount = 0;
    // Insert messages into Kafka.
    try (KafkaProducer<String, byte[]> producer =
        broker.createProducer(new StringSerializer(), new ByteArraySerializer(), null)) {
      for (LogMessage msg : messages) {

        // Kafka producer creates only a partition 0 on first message. So, set the partition to 0
        // always.
        Future<RecordMetadata> result =
            producer.send(
                new ProducerRecord<>(
                    kafkaTopic,
                    partitionId,
                    String.valueOf(indexedCount),
                    fromLogMessage(msg, indexedCount).toByteArray()));

        RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(kafkaTopic);
        indexedCount++;
      }
    }
    LOG.info(
        "Total messages produced={} to topic={} and partition={}", indexedCount, kafkaTopic, 0);
    return indexedCount;
  }

  // Create messages, format them into murron protobufs, write them to kafka
  public static int produceMessagesToKafka(
      EphemeralKafkaBroker broker, Instant startTime, String kafkaTopic, int partitionId)
      throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException {
    return produceMessagesToKafka(broker, startTime, kafkaTopic, partitionId, 100);
  }

  public static int produceMessagesToKafka(EphemeralKafkaBroker broker, Instant startTime)
      throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException {
    return produceMessagesToKafka(broker, startTime, TEST_KAFKA_TOPIC, 0);
  }

  public static Murron.MurronMessage fromLogMessage(LogMessage message, int offset)
      throws JsonProcessingException {
    String jsonStr = JsonUtil.writeAsString(message.source);
    return Murron.MurronMessage.newBuilder()
        .setTimestamp(message.timeSinceEpochMilli * 1000 * 1000)
        .setType(MessageUtil.TEST_DATA_SET_NAME)
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
    this(ALLOCATE_RANDOM_PORT);
  }

  public TestKafkaServer(int port) throws Exception {
    Properties brokerProperties = new Properties();
    // Set the number of default partitions for a kafka topic to 3 instead of 1.
    brokerProperties.put("num.partitions", "3");
    // Create a kafka broker
    broker = EphemeralKafkaBroker.create(port, ALLOCATE_RANDOM_PORT, brokerProperties);
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

  public void createTopicWithPartitions(String topic, int numPartitions)
      throws ExecutionException, InterruptedException {
    adminClient
        .createTopics(Collections.singleton(new NewTopic(topic, numPartitions, (short) 1)))
        .all()
        .get();
  }

  public CompletableFuture<Void> getBrokerStart() {
    return brokerStart;
  }

  public EphemeralKafkaBroker getBroker() {
    return broker;
  }

  public void close() throws ExecutionException, InterruptedException {
    adminClient.close();
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
