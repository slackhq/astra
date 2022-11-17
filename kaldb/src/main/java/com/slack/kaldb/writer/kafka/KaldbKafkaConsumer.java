package com.slack.kaldb.writer.kafka;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.KaldbConfig;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple wrapper class for Kafka Consumer. A kafka consumer is an infinite loop. So, it needs to
 * be run in a separate thread. Further, it is also important to shut down the consumer cleanly so
 * that we can guarantee that the data is indexed only once.
 */
public class KaldbKafkaConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbKafkaConsumer.class);
  public static final int KAFKA_POLL_TIMEOUT_MS = 250;
  private final LogMessageWriterImpl logMessageWriterImpl;

  public static KaldbKafkaConsumer fromConfig(
      KaldbConfigs.KafkaConfig kafkaCfg,
      LogMessageWriterImpl logMessageWriter,
      MeterRegistry meterRegistry) {
    return new KaldbKafkaConsumer(
        kafkaCfg.getKafkaTopic(),
        kafkaCfg.getKafkaTopicPartition(),
        kafkaCfg.getKafkaBootStrapServers(),
        kafkaCfg.getKafkaClientGroup(),
        kafkaCfg.getEnableKafkaAutoCommit(),
        kafkaCfg.getKafkaAutoCommitInterval(),
        kafkaCfg.getKafkaSessionTimeout(),
        logMessageWriter,
        meterRegistry);
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

  private KafkaConsumer<String, byte[]> kafkaConsumer;
  private final TopicPartition topicPartition;

  public static final String RECORDS_RECEIVED_COUNTER = "records_received";
  public static final String RECORDS_FAILED_COUNTER = "records_failed";
  private final Counter recordsReceivedCounter;
  private final Counter recordsFailedCounter;

  // TODO: Instead of passing each property as a field, consider defining props in config file.
  public KaldbKafkaConsumer(
      String kafkaTopic,
      String kafkaTopicPartitionStr,
      String kafkaBootStrapServers,
      String kafkaClientGroup,
      String enableKafkaAutoCommit,
      String kafkaAutoCommitInterval,
      String kafkaSessionTimeout,
      LogMessageWriterImpl logMessageWriterImpl,
      MeterRegistry meterRegistry) {

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

    recordsReceivedCounter = meterRegistry.counter(RECORDS_RECEIVED_COUNTER);
    recordsFailedCounter = meterRegistry.counter(RECORDS_FAILED_COUNTER);

    this.logMessageWriterImpl = logMessageWriterImpl;

    // Create kafka consumer
    Properties consumerProps =
        makeKafkaConsumerProps(
            kafkaBootStrapServers,
            kafkaClientGroup,
            enableKafkaAutoCommit,
            kafkaAutoCommitInterval,
            kafkaSessionTimeout);
    kafkaConsumer = new KafkaConsumer<>(consumerProps);
    new KafkaClientMetrics(kafkaConsumer).bindTo(meterRegistry);
  }

  /** Start consuming the partition from an offset. */
  public void prepConsumerForConsumption(long startOffset) {
    LOG.info("Starting kafka consumer for partition:{}.", topicPartition.partition());

    // Consume from a partition.
    kafkaConsumer.assign(Collections.singletonList(topicPartition));
    LOG.info("Assigned to topicPartition: {}", topicPartition);
    // Offset is negative when the partition was not consumed before, so start consumption from
    // beginning of the stream. If the offset is positive, start consuming from there.
    if (startOffset > 0) {
      kafkaConsumer.seek(topicPartition, startOffset);
    } else {
      kafkaConsumer.seekToBeginning(List.of(topicPartition));
    }
    LOG.info("Starting consumption for {} at offset: {}", topicPartition, startOffset);
  }

  public void close() {
    LOG.info("Closing kafka consumer for partition:{}", topicPartition);
    kafkaConsumer.close(KaldbConfig.DEFAULT_START_STOP_DURATION);
    LOG.info("Closed kafka consumer for partition:{}", topicPartition);
  }

  public long getEndOffSetForPartition() {
    return getEndOffSetForPartition(topicPartition);
  }

  public long getEndOffSetForPartition(TopicPartition topicPartition) {
    return kafkaConsumer.endOffsets(Collections.singletonList(topicPartition)).get(topicPartition);
  }

  public long getConsumerPositionForPartition() {
    return getConsumerPositionForPartition(topicPartition);
  }

  public long getConsumerPositionForPartition(TopicPartition topicPartition) {
    return kafkaConsumer.position(topicPartition);
  }

  public void consumeMessages() throws IOException {
    consumeMessages(KAFKA_POLL_TIMEOUT_MS);
  }

  public void consumeMessages(final long kafkaPollTimeoutMs) throws IOException {
    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.ofMillis(kafkaPollTimeoutMs));
    int recordCount = records.count();
    LOG.debug("Fetched records={} from partition:{}", recordCount, topicPartition.partition());
    if (recordCount > 0) {
      recordsReceivedCounter.increment(recordCount);
      int recordFailures = 0;
      for (ConsumerRecord<String, byte[]> record : records) {
        if (!logMessageWriterImpl.insertRecord(record)) recordFailures++;
      }
      recordsFailedCounter.increment(recordFailures);
      LOG.debug(
          "Processed {} records. Success: {}, Failed: {}",
          recordCount,
          recordCount - recordFailures,
          recordFailures);
    }
  }

  /**
   * The default offer method on array blocking queue class fails an insert an element when the
   * queue is full. So, we override the offer method here to wait when inserting the item until the
   * queue has enough capacity again. While this blocking behaviour is not ideal in the general
   * case, in this specific case, the blocking call acts as a back pressure mechanism pausing the
   * kafka message consumption from the broker.
   */
  private static class BlockingArrayBlockingQueue<E> extends ArrayBlockingQueue<E> {
    public BlockingArrayBlockingQueue(int capacity) {
      super(capacity);
    }

    @Override
    public boolean offer(E element) {
      try {
        return super.offer(element, Long.MAX_VALUE, TimeUnit.MINUTES);
      } catch (InterruptedException ex) {
        LOG.error("Exception in blocking array queue", ex);
        return false;
      }
    }
  }

  /**
   * Consume messages between the given start and end offset as fast as possible. This method is
   * called in the catchup indexer whose operations are idempotent. Further, we want to index the
   * data using all the available cpu power, so we should index it from multiple threads. But kafka
   * consumer is not thread safe, instead of calling the consumer from multiple threads, we will
   * decouple the consumption from processing using a blocking queue as recommended by the kafka
   * documentation.
   */
  public boolean consumeMessagesBetweenOffsetsInParallel(
      final long kafkaPollTimeoutMs, final long startOffsetInclusive, final long endOffsetInclusive)
      throws InterruptedException {
    final int maxPoolSize = 16;
    final int poolSize = Math.min(Runtime.getRuntime().availableProcessors() * 2, maxPoolSize);
    LOG.info("Pool size for queue is: {}", poolSize);

    // TODO: Track and log errors and success better.
    BlockingArrayBlockingQueue<Runnable> queue = new BlockingArrayBlockingQueue<>(100);
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            poolSize,
            poolSize,
            0L,
            TimeUnit.MILLISECONDS,
            queue,
            new ThreadFactoryBuilder()
                .setUncaughtExceptionHandler(
                    (t, e) ->
                        LOG.error("Exception in recovery task on thread {}: {}", t.getName(), e))
                .setNameFormat("recovery-task-%d")
                .build());

    final long messagesToIndex = endOffsetInclusive - startOffsetInclusive;
    long messagesIndexed = 0;
    while (messagesIndexed <= messagesToIndex) {
      ConsumerRecords<String, byte[]> records =
          kafkaConsumer.poll(Duration.ofMillis(kafkaPollTimeoutMs));
      int recordCount = records.count();
      LOG.debug("Fetched records={} from partition:{}", recordCount, topicPartition.partition());
      if (recordCount > 0) {
        messagesIndexed += recordCount;
        executor.execute(
            () -> {
              LOG.info("Ingesting batch: [{}/{}]", topicPartition.partition(), recordCount);
              for (ConsumerRecord<String, byte[]> record : records) {
                if (startOffsetInclusive >= 0 && record.offset() < startOffsetInclusive) {
                  throw new IllegalArgumentException(
                      "Record is before start offset range: " + startOffsetInclusive);
                }
                if (endOffsetInclusive >= 0 && record.offset() > endOffsetInclusive) {
                  throw new IllegalArgumentException(
                      "Record is after end offset range: " + endOffsetInclusive);
                }
                try {
                  if (logMessageWriterImpl.insertRecord(record)) {
                    recordsReceivedCounter.increment();
                  } else {
                    recordsFailedCounter.increment();
                  }
                } catch (IOException e) {
                  LOG.error(
                      "Encountered exception processing batch [{}/{}]: {}",
                      topicPartition.partition(),
                      recordCount,
                      e);
                }
              }
              // TODO: Not all threads are printing this message.
              LOG.info(
                  "Finished ingesting batch: [{}/{}]", topicPartition.partition(), recordCount);
            });
        LOG.debug("Queued");
      } else {
        // temporary diagnostic logging
        LOG.info("Encountered zero-record batch from partition {}", topicPartition.partition());
      }
    }
    executor.shutdown();
    LOG.info("Shut down");
    return executor.awaitTermination(1, TimeUnit.MINUTES);
  }

  @VisibleForTesting
  public KafkaConsumer<String, byte[]> getKafkaConsumer() {
    return kafkaConsumer;
  }

  @VisibleForTesting
  void setKafkaConsumer(KafkaConsumer<String, byte[]> kafkaConsumer) {
    this.kafkaConsumer = kafkaConsumer;
  }
}
