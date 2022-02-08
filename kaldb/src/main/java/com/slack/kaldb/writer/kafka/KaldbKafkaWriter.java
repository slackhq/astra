package com.slack.kaldb.writer.kafka;

import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.writer.MessageWriter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class consumes the data from Kafka and indexes it into Kaldb using a message writer. */
public class KaldbKafkaWriter extends KaldbKafkaConsumer {

  public static KaldbKafkaWriter fromConfig(
      MessageWriter messageWriter, MeterRegistry meterRegistry) {
    KaldbConfigs.KafkaConfig kafkaCfg = KaldbConfig.get().getKafkaConfig();
    return fromConfig(kafkaCfg, messageWriter, meterRegistry);
  }

  public static KaldbKafkaWriter fromConfig(
      KaldbConfigs.KafkaConfig kafkaCfg, MessageWriter messageWriter, MeterRegistry meterRegistry) {
    return new KaldbKafkaWriter(
        meterRegistry,
        kafkaCfg.getKafkaTopic(),
        kafkaCfg.getKafkaTopicPartition(),
        kafkaCfg.getKafkaBootStrapServers(),
        kafkaCfg.getKafkaClientGroup(),
        kafkaCfg.getEnableKafkaAutoCommit(),
        kafkaCfg.getKafkaAutoCommitInterval(),
        kafkaCfg.getKafkaSessionTimeout(),
        messageWriter);
  }

  private static final Logger LOG = LoggerFactory.getLogger(KaldbKafkaWriter.class);
  private final MessageWriter messageWriter;

  public static final String RECORDS_RECEIVED_COUNTER = "records_received";
  public static final String RECORDS_FAILED_COUNTER = "records_failed";
  private final Counter recordsReceivedCounter;
  private final Counter recordsFailedCounter;

  // TODO: Convert kafka fields into a Properties Object.
  public KaldbKafkaWriter(
      MeterRegistry meterRegistry,
      String kafkaTopic,
      String kafkaTopicPartitionStr,
      String kafkaBootStrapServers,
      String kafkaClientGroup,
      String enableKafkaAutoCommit,
      String kafkaAutoCommitInterval,
      String kafkaSessionTimeout,
      MessageWriter messageWriter) {
    super(
        kafkaTopic,
        kafkaTopicPartitionStr,
        kafkaBootStrapServers,
        kafkaClientGroup,
        enableKafkaAutoCommit,
        kafkaAutoCommitInterval,
        kafkaSessionTimeout);
    this.messageWriter = messageWriter;
    recordsReceivedCounter = meterRegistry.counter(RECORDS_RECEIVED_COUNTER);
    recordsFailedCounter = meterRegistry.counter(RECORDS_FAILED_COUNTER);
  }

  @Override
  void consumeMessages(long kafkaPollTimeoutMs) throws IOException {
    KafkaConsumer<String, byte[]> consumer = getConsumer();

    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(kafkaPollTimeoutMs));
    int recordCount = records.count();
    LOG.debug("Fetched records={}", recordCount);
    if (recordCount > 0) {
      recordsReceivedCounter.increment(recordCount);
      int recordFailures = 0;
      for (ConsumerRecord<String, byte[]> record : records) {
        if (!messageWriter.insertRecord(record)) recordFailures++;
      }
      recordsFailedCounter.increment(recordFailures);
      LOG.debug(
          "Processed {} records. Success: {}, Failed: {}",
          recordCount,
          recordCount - recordFailures,
          recordFailures);
    }
  }
}
