package com.slack.kaldb.writer.kafka;

import com.google.common.base.Stopwatch;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.writer.MessageWriter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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
        kafkaCfg.getKafkaOffsetPosition(),
        messageWriter,
        END_OFFSET_STALE_DELAY_SECS);
  }

  private static final Logger LOG = LoggerFactory.getLogger(KaldbKafkaWriter.class);
  private final MessageWriter messageWriter;
  private final Stopwatch endOffsetTimer;
  private final long endOffsetStaleSecs;
  private long cachedPartitionEndOffset;

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
      String kafkaOffsetPosition,
      MessageWriter messageWriter,
      long endOffsetStaleSecs) {
    super(
        kafkaTopic,
        kafkaTopicPartitionStr,
        kafkaBootStrapServers,
        kafkaClientGroup,
        enableKafkaAutoCommit,
        kafkaAutoCommitInterval,
        kafkaSessionTimeout,
        kafkaOffsetPosition);
    this.messageWriter = messageWriter;
    this.endOffsetTimer = Stopwatch.createStarted();
    this.endOffsetStaleSecs = endOffsetStaleSecs;
    this.cachedPartitionEndOffset = 0;
    recordsReceivedCounter = meterRegistry.counter(RECORDS_RECEIVED_COUNTER);
    recordsFailedCounter = meterRegistry.counter(RECORDS_FAILED_COUNTER);
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
