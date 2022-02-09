package com.slack.kaldb.writer.kafka;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;

import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple wrapper class for Kafka Consumer. A kafka consumer is an infinite loop. So, it needs to
 * be run in a separate thread. Further, it is also important to shut down the consumer cleanly so
 * that we can guarantee that the data is indexed only once.
 */
public class KaldbKafkaConsumer2 {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbKafkaConsumer2.class);

  public static KaldbKafkaConsumer2 fromConfig(KaldbConfigs.KafkaConfig kafkaCfg) {
    return new KaldbKafkaConsumer2(
        kafkaCfg.getKafkaTopic(),
        kafkaCfg.getKafkaTopicPartition(),
        kafkaCfg.getKafkaBootStrapServers(),
        kafkaCfg.getKafkaClientGroup(),
        kafkaCfg.getEnableKafkaAutoCommit(),
        kafkaCfg.getKafkaAutoCommitInterval(),
        kafkaCfg.getKafkaSessionTimeout());
  }

  private static Properties makeKafkaConsumerProps(
      String kafkaBootStrapServers,
      String kafkaClientGroup,
      String enableKafkaAutoCommit,
      String kafkaAutoCommitInterval,
      String kafkaSessionTimeout) {

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaClientGroup);
    // TODO: Consider committing manual consumer offset?
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableKafkaAutoCommit);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, kafkaAutoCommitInterval);
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    // TODO: Using ByteArrayDeserializer since it's most primitive and performant. Replace it if
    // not.
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    // TODO: Does the session timeout matter in assign?
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaSessionTimeout);
    return props;
  }

  // TODO: Keep this private?
  public KafkaConsumer<String, byte[]> getConsumer() {
    return consumer;
  }

  private final KafkaConsumer<String, byte[]> consumer;
  private final TopicPartition topicPartition;
  private final Properties consumerProps;

  // TODO: Instead of passing each property as a field, consider defining props in config file.
  public KaldbKafkaConsumer2(
      String kafkaTopic,
      String kafkaTopicPartitionStr,
      String kafkaBootStrapServers,
      String kafkaClientGroup,
      String enableKafkaAutoCommit,
      String kafkaAutoCommitInterval,
      String kafkaSessionTimeout) {

    checkArgument(
        kafkaTopic != null && !kafkaTopic.isEmpty(), "Kafka topic can't be null or " + "empty");
    checkArgument(
        kafkaBootStrapServers != null && !kafkaBootStrapServers.isEmpty(),
        "Kafka bootstrap server list can't be null or empty");
    checkArgument(
        kafkaClientGroup != null && !kafkaClientGroup.isEmpty(),
        "Kafka client group can't be null or empty");
    checkArgument(
        enableKafkaAutoCommit != null && !enableKafkaAutoCommit.isEmpty(),
        "Kafka enable auto commit can't be null or empty");
    checkArgument(
        kafkaAutoCommitInterval != null && !kafkaAutoCommitInterval.isEmpty(),
        "Kafka auto commit interval can't be null or empty");
    checkArgument(
        kafkaSessionTimeout != null && !kafkaSessionTimeout.isEmpty(),
        "Kafka session timeout can't be null or empty");
    checkArgument(
        kafkaTopicPartitionStr != null && !kafkaTopicPartitionStr.isEmpty(),
        "Kafka topic partition can't be null or empty");

    LOG.info(
        "Kafka params are: kafkaTopicName: {}, kafkaTopicPartition: {}, "
            + "kafkaBootstrapServers:{}, kafkaClientGroup: {}, kafkaAutoCommit:{}, "
            + "kafkaAutoCommitInterval: {}, kafkaSessionTimeout: {}",
        kafkaTopic,
        kafkaTopicPartitionStr,
        kafkaBootStrapServers,
        kafkaClientGroup,
        enableKafkaAutoCommit,
        kafkaAutoCommitInterval,
        kafkaSessionTimeout);

    int kafkaTopicPartition = parseInt(kafkaTopicPartitionStr);
    topicPartition = new TopicPartition(kafkaTopic, kafkaTopicPartition);

    // Create kafka consumer
    consumerProps =
        makeKafkaConsumerProps(
            kafkaBootStrapServers,
            kafkaClientGroup,
            enableKafkaAutoCommit,
            kafkaAutoCommitInterval,
            kafkaSessionTimeout);
    consumer = new KafkaConsumer<>(consumerProps);
  }

  public void prepConsumerForConsumption(long startOffset) {
    LOG.info("Starting kafka consumer.");

    // Consume from a partition.
    // TODO: Use a sticky consumer instead of assign?
    consumer.assign(Collections.singletonList(topicPartition));
    LOG.info("Assigned to topicPartition: {}", topicPartition);
    // TODO: Only when statOffset is positive. Also, consumer group exists.
    consumer.seek(topicPartition, startOffset);
    LOG.info("Starting consumption for {} at offset: {}", topicPartition, startOffset);
  }

  public void close() {
    LOG.info("Closing kafka consumer.");
    consumer.close(KaldbConfig.DEFAULT_START_STOP_DURATION);
    LOG.info("Closed kafka consumer.");
  }

  public long getLatestOffSet() {
    return consumer.endOffsets(Collections.singletonList(topicPartition)).get(topicPartition);
  }
}
