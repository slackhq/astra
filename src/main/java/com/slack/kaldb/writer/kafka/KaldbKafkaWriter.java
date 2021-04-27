package com.slack.kaldb.writer.kafka;

import com.google.common.base.Stopwatch;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.writer.MessageWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class consumes the data from Kafka and indexes it into Kaldb using a message writer. */
public class KaldbKafkaWriter extends KaldbKafkaConsumer {

  public static long END_OFFSET_STALE_DELAY_SECS = 10;

  public static KaldbKafkaWriter fromConfig(MessageWriter messageWriter) {
    KaldbConfigs.KafkaConfig kafkaCfg = KaldbConfig.get().getKafkaConfig();
    return fromConfig(kafkaCfg, messageWriter);
  }

  public static KaldbKafkaWriter fromConfig(
      KaldbConfigs.KafkaConfig kafkaCfg, MessageWriter messageWriter) {
    return new KaldbKafkaWriter(
        kafkaCfg.getKafkaTopic(),
        kafkaCfg.getKafkaTopicPartition(),
        kafkaCfg.getKafkaBootStrapServers(),
        kafkaCfg.getKafkaClientGroup(),
        kafkaCfg.getEnableKafkaAutoCommit(),
        kafkaCfg.getKafkaAutoCommitInterval(),
        kafkaCfg.getKafkaSessionTimeout(),
        messageWriter,
        END_OFFSET_STALE_DELAY_SECS);
  }

  private static final Logger LOG = LoggerFactory.getLogger(KaldbKafkaWriter.class);
  private final MessageWriter messageWriter;
  private final Stopwatch endOffsetTimer;
  private final long endOffsetStaleSecs;
  private long cachedPartitionEndOffset;

  // TODO: Add counters for failures, success, messages ingested.

  // TODO: Convert kafka fields into a Properties Object.
  public KaldbKafkaWriter(
      String kafkaTopic,
      String kafkaTopicPartitionStr,
      String kafkaBootStrapServers,
      String kafkaClientGroup,
      String enableKafkaAutoCommit,
      String kafkaAutoCommitInterval,
      String kafkaSessionTimeout,
      MessageWriter messageWriter,
      long endOffsetStaleSecs) {
    super(
        kafkaTopic,
        kafkaTopicPartitionStr,
        kafkaBootStrapServers,
        kafkaClientGroup,
        enableKafkaAutoCommit,
        kafkaAutoCommitInterval,
        kafkaSessionTimeout);
    this.messageWriter = messageWriter;
    this.endOffsetTimer = Stopwatch.createStarted();
    this.endOffsetStaleSecs = endOffsetStaleSecs;
    this.cachedPartitionEndOffset = 0;
  }

  private long getLatestOffSet() {
    return getConsumer()
        .endOffsets(Collections.singletonList(getTopicPartition()))
        .get(getTopicPartition());
  }

  // TODO: Move offset catch up into a separate class?
  private long getCachedLatestOffset() {
    if (cachedPartitionEndOffset == 0
        || endOffsetTimer.elapsed(TimeUnit.SECONDS) >= endOffsetStaleSecs) {
      cachedPartitionEndOffset = getLatestOffSet();
      endOffsetTimer.reset().start();
    }
    return cachedPartitionEndOffset;
  }

  @Override
  void consumeMessages(long kafkaPollTimeoutMs) throws IOException {
    KafkaConsumer<String, byte[]> consumer = getConsumer();

    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(kafkaPollTimeoutMs));
    LOG.debug("Fetched records." + records.count());

    int recordCount = records.count();
    int recordFailures = 0;
    for (ConsumerRecord<String, byte[]> record : records) {
      if (!messageWriter.insertRecord(record)) recordFailures++;
    }

    LOG.info(
        "Processed {} records. Success: {}, Failed: {}",
        recordCount,
        recordCount - recordFailures,
        recordFailures);
  }
}
