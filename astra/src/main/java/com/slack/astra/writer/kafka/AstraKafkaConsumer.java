package com.slack.astra.writer.kafka;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.slack.astra.util.TimeUtils.nanosToMillis;
import static java.lang.Integer.parseInt;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.server.AstraConfig;
import com.slack.astra.writer.KafkaUtils;
import com.slack.astra.writer.LogMessageWriterImpl;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple wrapper class for Kafka Consumer. A kafka consumer is an infinite loop. So, it needs to
 * be run in a separate thread. Further, it is also important to shut down the consumer cleanly so
 * that we can guarantee that the data is indexed only once.
 */
public class AstraKafkaConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(AstraKafkaConsumer.class);
  public static final int KAFKA_POLL_TIMEOUT_MS = 250;
  private final LogMessageWriterImpl logMessageWriterImpl;
  private static final String[] REQUIRED_CONFIGS = {ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG};

  private static final Set<String> OVERRIDABLE_CONFIGS =
      Set.of(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);

  @VisibleForTesting
  public static Properties makeKafkaConsumerProps(AstraConfigs.KafkaConfig kafkaConfig) {

    String kafkaBootStrapServers = kafkaConfig.getKafkaBootStrapServers();
    String kafkaClientGroup = kafkaConfig.getKafkaClientGroup();
    String enableKafkaAutoCommit = kafkaConfig.getEnableKafkaAutoCommit();
    String kafkaAutoCommitInterval = kafkaConfig.getKafkaAutoCommitInterval();
    String kafkaSessionTimeout = kafkaConfig.getKafkaSessionTimeout();

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaClientGroup);
    // TODO: Consider committing manual consumer offset?
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableKafkaAutoCommit);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, kafkaAutoCommitInterval);
    // TODO: Does the session timeout matter in assign?
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaSessionTimeout);

    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    // we rely on the fail-fast behavior of 'auto.offset.reset = none' to handle scenarios
    // with recovery tasks where the offsets are no longer available in Kafka
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

    // don't override the properties that we have already set explicitly using named properties
    for (Map.Entry<String, String> additionalProp :
        kafkaConfig.getAdditionalPropsMap().entrySet()) {
      props =
          KafkaUtils.maybeOverrideProps(
              props,
              additionalProp.getKey(),
              additionalProp.getValue(),
              OVERRIDABLE_CONFIGS.contains(additionalProp.getKey()));
    }
    return props;
  }

  private KafkaConsumer<String, byte[]> kafkaConsumer;
  private final TopicPartition topicPartition;

  public static final String RECORDS_RECEIVED_COUNTER = "records_received";
  public static final String RECORDS_FAILED_COUNTER = "records_failed";
  private final Counter recordsReceivedCounter;
  private final Counter recordsFailedCounter;

  public AstraKafkaConsumer(
      AstraConfigs.KafkaConfig kafkaConfig,
      LogMessageWriterImpl logMessageWriterImpl,
      MeterRegistry meterRegistry) {

    topicPartition =
        getTopicPartition(kafkaConfig.getKafkaTopic(), kafkaConfig.getKafkaTopicPartition());
    recordsReceivedCounter = meterRegistry.counter(RECORDS_RECEIVED_COUNTER);
    recordsFailedCounter = meterRegistry.counter(RECORDS_FAILED_COUNTER);
    this.logMessageWriterImpl = logMessageWriterImpl;

    // Create kafka consumer
    Properties consumerProps = makeKafkaConsumerProps(kafkaConfig);
    validateKafkaConfig(consumerProps);

    kafkaConsumer = new KafkaConsumer<>(consumerProps);
    new KafkaClientMetrics(kafkaConsumer).bindTo(meterRegistry);
  }

  private void validateKafkaConfig(Properties props) {
    for (String property : props.stringPropertyNames()) {
      Preconditions.checkArgument(
          props.getProperty(property) != null && !props.getProperty(property).isEmpty(),
          "Property %s cannot be null or empty", property);
    }

    // Check required configs.
    for (String requiredConfig : REQUIRED_CONFIGS) {
      checkNotNull(
          props.getProperty(requiredConfig),
          "Property %s is required but not provided", requiredConfig);
    }

    StringBuilder propertiesBuilder = new StringBuilder("Kafka params are: ");
    props
        .stringPropertyNames()
        .forEach(key -> propertiesBuilder.append(key).append(": ").append(props.getProperty(key)));
    LOG.info(propertiesBuilder.toString());
  }

  public static TopicPartition getTopicPartition(String kafkaTopic, String kafkaTopicPartitionStr) {
    int kafkaTopicPartition = parseInt(kafkaTopicPartitionStr);
    return new TopicPartition(kafkaTopic, kafkaTopicPartition);
  }

  /** Start consuming the partition from an offset. */
  public void prepConsumerForConsumption(long startOffset) {
    LOG.info("Starting kafka consumer for partition:{}.", topicPartition.partition());

    // Consume from a partition.
    kafkaConsumer.assign(Collections.singletonList(topicPartition));
    LOG.info("Assigned to topicPartition: {}", topicPartition);
    // Offset is negative when the partition was not consumed before, so start consumption from
    // there
    if (startOffset > 0) {
      kafkaConsumer.seek(topicPartition, startOffset);
    } else {
      kafkaConsumer.seekToBeginning(List.of(topicPartition));
    }
    LOG.info("Starting consumption for {} at offset: {}", topicPartition, startOffset);
  }

  public void close() {
    LOG.info("Closing kafka consumer for partition:{}", topicPartition);
    kafkaConsumer.close(AstraConfig.DEFAULT_START_STOP_DURATION);
    LOG.info("Closed kafka consumer for partition:{}", topicPartition);
  }

  public long getBeginningOffsetForPartition() {
    return getBeginningOffsetForPartition(topicPartition);
  }

  public long getBeginningOffsetForPartition(TopicPartition topicPartition) {
    return kafkaConsumer
        .beginningOffsets(Collections.singletonList(topicPartition))
        .get(topicPartition);
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

  protected ConsumerRecords<String, byte[]> pollWithRetry(final long kafkaPollTimeoutMs) {
    ConsumerRecords<String, byte[]> records = null;
    OffsetOutOfRangeException kafkaError = null;
    for (int i = 0; i < 4; i++) {
      try {
        records = kafkaConsumer.poll(Duration.ofMillis(kafkaPollTimeoutMs));
        kafkaError = null;
        break;
      } catch (OffsetOutOfRangeException ex) {
        kafkaError = ex;
        LOG.warn(String.format("Caught unexpected exception, retry number: %d", i), ex);
        try {
          Thread.sleep((i + 1) * 3000L);
        } catch (InterruptedException exex) {
          LOG.warn("Caught unexpected exception during sleep.", exex);
        }
      }
    }
    if (kafkaError != null) {
      throw kafkaError;
    }
    return records;
  }

  public void consumeMessages(final long kafkaPollTimeoutMs) throws IOException {
    ConsumerRecords<String, byte[]> records = pollWithRetry(kafkaPollTimeoutMs);
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
  static class BlockingArrayBlockingQueue<E> extends ArrayBlockingQueue<E> {
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
    final AtomicLong messagesOutsideOffsetRange = new AtomicLong(0);
    while (messagesIndexed <= messagesToIndex) {
      ConsumerRecords<String, byte[]> records = pollWithRetry(kafkaPollTimeoutMs);
      int recordCount = records.count();
      LOG.debug("Fetched records={} from partition:{}", recordCount, topicPartition);
      if (recordCount > 0) {
        messagesIndexed += recordCount;
        executor.execute(
            () -> {
              long startTime = System.nanoTime();
              try {
                LOG.debug("Ingesting batch from {} with {} records", topicPartition, recordCount);
                for (ConsumerRecord<String, byte[]> record : records) {
                  if (startOffsetInclusive >= 0 && record.offset() < startOffsetInclusive) {
                    messagesOutsideOffsetRange.incrementAndGet();
                    recordsFailedCounter.increment();
                  } else if (endOffsetInclusive >= 0 && record.offset() > endOffsetInclusive) {
                    messagesOutsideOffsetRange.incrementAndGet();
                    recordsFailedCounter.increment();
                  } else {
                    try {
                      if (logMessageWriterImpl.insertRecord(record)) {
                        recordsReceivedCounter.increment();
                      } else {
                        recordsFailedCounter.increment();
                      }
                    } catch (IOException e) {
                      LOG.error(
                          "Encountered exception processing batch from {} with {} records: {}",
                          topicPartition,
                          recordCount,
                          e);
                    }
                  }
                }
                LOG.debug(
                    "Finished ingesting batch from {} with {} records",
                    topicPartition,
                    recordCount);
              } finally {
                long endTime = System.nanoTime();
                LOG.info(
                    "Batch from {} with {} records completed in {}ms",
                    topicPartition,
                    recordCount,
                    nanosToMillis(endTime - startTime));
              }
            });
        LOG.debug("Queued");
      } else {
        // temporary diagnostic logging
        LOG.debug("Encountered zero-record batch from partition {}", topicPartition);
      }
    }
    if (messagesOutsideOffsetRange.get() > 0) {
      LOG.info(
          "Messages permanently dropped because they were outside the expected offset ranges for the recovery task: {}",
          messagesOutsideOffsetRange.get());
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
