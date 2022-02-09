package com.slack.kaldb.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.config.KaldbConfig.dataTransformerMap;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.slack.kaldb.chunkManager.ChunkRollOverException;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.util.RuntimeHalterImpl;
import com.slack.kaldb.writer.LogMessageTransformer;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.kaldb.writer.kafka.KaldbKafkaConsumer2;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KaldbIndexer sets up an indexer that indexes the log messages.
 *
 * <p>This indexer should be testable via junit tests. So, it should have the least number of deps
 * in it's constructor.
 *
 * <p>Design should be extensible so we can run as separate components or all components in a single
 * binary.
 *
 * <p>queryService chunkCleaner chunkManager ingestionControlService (reads kafka, passes
 * chunkManager)
 *
 * <p>* encapsulate kafka consumer * offset logic * NOT chunk manager * start * math to determine
 * offsets * start consumer * stop * close consumer * shutdown chunk manager * shut down indexer
 * service.
 *
 * <p>indexer.shutdown(); queryService > pending queries chunkCleaner > immediate ingestionControl >
 * immediate chunkManager > close, flush everything
 *
 * <p>KaldbGuavaServices.shutdown();
 *
 * <p>TODO: Run in it's own thread?
 */
public class KaldbIndexer2 extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbIndexer2.class);

  private final MetadataStore metadataStore;
  private final MeterRegistry meterRegistry;
  private final KaldbConfigs.IndexerConfig indexerConfig;
  private final KaldbConfigs.KafkaConfig kafkaConfig;
  private final KaldbKafkaConsumer2 kafkaConsumer;

  private final IndexingChunkManager<LogMessage> chunkManager;

  public static final String RECORDS_RECEIVED_COUNTER = "records_received";
  public static final String RECORDS_FAILED_COUNTER = "records_failed";
  private final Counter recordsReceivedCounter;
  private final Counter recordsFailedCounter;
  private LogMessageWriterImpl logMessageWriterImpl;

  /**
   * This class contains the code to needed to run a single instance of an Kaldb indexer. A single
   * instance of Kaldb indexer, indexes data from kafka into the chunk manager and provides an API
   * to search that data.
   *
   * <p>In addition, this class also contains the code to gracefully start and shutdown the server.
   *
   * <p>The only way we can ensure durability of data is when the data _and_ metadata are stored
   * reliably. So, on a clean indexer shutdown we need to ensure that as much of indexed data and
   * metadata is stored reliably. Otherwise, on an indexer shutdown we would end up re-indexing the
   * data which would result in a lot of wasted work. *
   *
   * <p>On an indexer restart, we should start indexing at a last known good offset for that
   * partition. If a last known good offset doesn't exist since we are consuming for the first time
   * then we start with head. If the offset exists but the offset expired, we are in a whole world
   * of pain. The best option may to start indexing at oldest. Or we can also start indexing at
   * head.
   *
   * <p>Currently, we don't have a durable metadata store and the kafka consumer offset acts as a
   * weak place holder. On an indexer shutdown it is very important that we ensure that we persisted
   * * the offset of the data correctly. So we can pick up from the same location and start from
   * that place.
   *
   * <p>The best way to close an indexer is the following steps: stop ingestion, index the ingested
   * messages, persist the indexed messages and metadata successfully and then close the
   * chunkManager and then the consumer,
   */
  public KaldbIndexer2(
      IndexingChunkManager<LogMessage> chunkManager,
      MetadataStore metadataStore,
      KaldbConfigs.IndexerConfig indexerConfig,
      KaldbConfigs.KafkaConfig kafkaConfig,
      MeterRegistry meterRegistry) {
    checkNotNull(chunkManager, "Chunk manager can't be null");
    this.chunkManager = chunkManager;
    this.metadataStore = metadataStore;
    this.indexerConfig = indexerConfig;
    this.kafkaConfig = kafkaConfig;
    this.meterRegistry = meterRegistry;
    this.kafkaConsumer = KaldbKafkaConsumer2.fromConfig(kafkaConfig);

    recordsReceivedCounter = meterRegistry.counter(RECORDS_RECEIVED_COUNTER);
    recordsFailedCounter = meterRegistry.counter(RECORDS_FAILED_COUNTER);
  }

  @Override
  protected void startUp() throws Exception {
    // Indexer start
    LOG.info("Starting indexing into Kaldb.");

    // Run indexer pre-start operation like determining start offset and optionally create a
    // recovery task.
    long startOffset = indexerPreStart();

    // TODO: Set this value as the starting offset for Kafka consumer.
    LogMessageTransformer messageTransformer =
        dataTransformerMap.get(indexerConfig.getDataTransformer());
    logMessageWriterImpl = new LogMessageWriterImpl(chunkManager, messageTransformer);
    // TODO: Wait for other services to be ready before we start consuming?
    // kafkaWriter.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  // Indexer pre-start
  // Clean up stale indexer tasks and optionally create a recovery task
  private long indexerPreStart() throws Exception {
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    RecoveryTaskMetadataStore recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(metadataStore, false);

    // TODO: Pass these values in?
    String partitionId = kafkaConfig.getKafkaTopicPartition();
    long maxOffsetDelay = indexerConfig.getMaxOffsetDelay();

    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskMetadataStore,
            partitionId,
            maxOffsetDelay,
            meterRegistry);
    // TODO: Pass this valve in?
    // TODO: Check consumer group exists.
    long currentHeadOffsetForPartition = kafkaConsumer.getLatestOffSet();
    long startOffset = recoveryTaskCreator.determineStartingOffset(currentHeadOffsetForPartition);

    // Close these stores since we don't need them after preStart.
    snapshotMetadataStore.close();
    recoveryTaskMetadataStore.close();
    return startOffset;
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      // TODO: Refactor as a function into KaldbKafkaConsumer2?
      try {
        long kafkaPollTimeoutMs = 100;
        consumeMessages(kafkaPollTimeoutMs);
      } catch (RejectedExecutionException e) {
        // This case shouldn't happen since there is only one thread queuing tasks here and we check
        // that the queue is empty before polling kafka.
        LOG.error("Rejected execution shouldn't happen ", e);
      } catch (ChunkRollOverException | IOException e) {
        // Once we hit these exceptions, we likely have an issue related to storage. So, terminate
        // the program, since consuming more messages from Kafka would only make the issue worse.
        LOG.error("FATAL: Encountered an unrecoverable storage exception.", e);
        new RuntimeHalterImpl().handleFatal(e);
      } catch (Exception e) {
        LOG.error("FATAL: Unhandled exception ", e);
        new RuntimeHalterImpl().handleFatal(e);
      }
    }

    kafkaConsumer.close();
  }

  void consumeMessages(long kafkaPollTimeoutMs) throws IOException {
    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.getConsumer().poll(Duration.ofMillis(kafkaPollTimeoutMs));
    int recordCount = records.count();
    LOG.debug("Fetched records={}", recordCount);
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
   * TODO: Currently, we close the consumer at the same time as stopping indexing. It may be better
   * to separate those steps where we stop ingestion and then close the consumer separately. This
   * will help with cleaner indexing.
   */
  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down Kaldb indexer.");

    // Shutdown kafka consumer cleanly and then the chunkmanager so we can be sure, we have indexed
    // the data we ingested.
    kafkaConsumer.close();

    //    kafkaWriter.stopAsync();
    //    try {
    //      LOG.info("Waiting for Kafka consumer to close.");
    //      // Use a more configurable timeout value.
    //      kafkaWriter.awaitTerminated(2, TimeUnit.SECONDS);
    //      if (!kafkaWriter.isRunning()) {
    //        LOG.info("Closed Kafka consumer cleanly");
    //      } else {
    //        LOG.warn("Kafka consumer was not closed cleanly");
    //      }
    //    } catch (TimeoutException e) {
    //      LOG.warn("Failed to close kafka consumer cleanly because of a timeout.", e);
    //    } catch (Exception e) {
    //      LOG.warn("Failed to close kafka consumer cleanly because of an exception.", e);
    //    }

    chunkManager.stopAsync();
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);

    LOG.info("Kaldb indexer is closed.");
  }

  @Override
  protected String serviceName() {
    return "kaldbIndexerService";
  }
}
