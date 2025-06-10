package com.slack.astra.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.slack.astra.chunkManager.ChunkRollOverException;
import com.slack.astra.chunkManager.IndexingChunkManager;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.util.RuntimeHalterImpl;
import com.slack.astra.writer.LogMessageWriterImpl;
import com.slack.astra.writer.kafka.AstraKafkaConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AstraIndexer creates an indexer to index the log data. The indexer also exposes a search api to
 * search the log data.
 */
public class AstraIndexer extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(AstraIndexer.class);

  private final AsyncCuratorFramework curatorFramework;
  private final MeterRegistry meterRegistry;
  private final AstraConfigs.MetadataStoreConfig metadataStoreConfig;
  private final AstraConfigs.IndexerConfig indexerConfig;
  private final AstraConfigs.KafkaConfig kafkaConfig;
  private final AstraKafkaConsumer kafkaConsumer;
  private final IndexingChunkManager<LogMessage> chunkManager;

  /**
   * This class contains the code to needed to run a single instance of an Astra indexer. A single
   * instance of Astra indexer, indexes data from kafka into the chunk manager and provides an API
   * to search that data.
   *
   * <p>In addition, this class also contains the code to gracefully start and shutdown the server.
   *
   * <p>The only way we can ensure durability of data is when the data _and_ metadata are stored
   * reliably. So, on a indexer shutdown we need to ensure that as much of indexed data and metadata
   * is stored reliably. Otherwise, on an indexer shutdown we would end up re-indexing the data
   * which would result in wasted work.
   *
   * <p>On an indexer restart, we should start indexing at a last known good offset for that
   * partition. The indexer pre-start job, ensures that we choose the correct start offset and end
   * offset for the indexer. Optionally, the pre start task also creates an recovery task if needed,
   * since the indexer may not be able to catch up.
   */
  public AstraIndexer(
      IndexingChunkManager<LogMessage> chunkManager,
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      AstraConfigs.IndexerConfig indexerConfig,
      AstraConfigs.KafkaConfig kafkaConfig,
      MeterRegistry meterRegistry) {
    checkNotNull(chunkManager, "Chunk manager can't be null");
    this.curatorFramework = curatorFramework;
    this.metadataStoreConfig = metadataStoreConfig;
    this.indexerConfig = indexerConfig;
    this.kafkaConfig = kafkaConfig;
    this.meterRegistry = meterRegistry;

    // Create a chunk manager
    this.chunkManager = chunkManager;
    // set up indexing pipelne
    LogMessageWriterImpl logMessageWriterImpl = new LogMessageWriterImpl(chunkManager);
    this.kafkaConsumer = new AstraKafkaConsumer(kafkaConfig, logMessageWriterImpl, meterRegistry);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Astra indexer.");
    long startOffset = indexerPreStart();
    // ensure the chunk manager is available to receive messages
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Set the Kafka offset and pre consumer for consumption.
    kafkaConsumer.prepConsumerForConsumption(startOffset);

    LOG.info("Started Astra indexer.");
  }

  /**
   * Indexer pre-start cleans up stale indexer tasks and optionally creates a recovery task if we
   * can't catch up.
   */
  private long indexerPreStart() throws Exception {
    LOG.info("Starting Astra indexer pre start.");
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);
    RecoveryTaskMetadataStore recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);

    String partitionId = kafkaConfig.getKafkaTopicPartition();
    long maxOffsetDelay = indexerConfig.getMaxOffsetDelayMessages();

    // TODO: Move this to it's own config var.
    final long maxMessagesPerRecoveryTask = indexerConfig.getMaxMessagesPerChunk();
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskMetadataStore,
            partitionId,
            maxOffsetDelay,
            maxMessagesPerRecoveryTask,
            meterRegistry);

    long currentEndOffsetForPartition = kafkaConsumer.getEndOffSetForPartition();
    long currentBeginningOffsetForPartition = kafkaConsumer.getBeginningOffsetForPartition();
    long startOffset =
        recoveryTaskCreator.determineStartingOffset(
            currentEndOffsetForPartition, currentBeginningOffsetForPartition, indexerConfig);

    // Close these stores since we don't need them after preStart.
    snapshotMetadataStore.close();
    recoveryTaskMetadataStore.close();
    LOG.info(
        "Completed Astra indexer pre start - currentEndOffsetForPartition {}, currentBeginningOffsetForPartition {}, startOffset {}",
        currentEndOffsetForPartition,
        currentBeginningOffsetForPartition,
        startOffset);

    return startOffset;
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      try {
        kafkaConsumer.consumeMessages();
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

  /**
   * The only way we can ensure durability of data is when the data _and_ metadata are stored
   * reliably. So, on a indexer shutdown we need to ensure that as much of indexed data and metadata
   * is stored reliably. Otherwise, on an indexer shutdown we would end up re-indexing the data
   * which would result in wasted work.
   *
   * <p>The best way to close an indexer is the following steps: stop ingestion, index the ingested
   * messages, persist the indexed messages and metadata successfully and then close the
   * chunkManager and then the consumer.
   */
  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down Astra indexer.");

    // Shutdown kafka consumer
    try {
      kafkaConsumer.close();
    } catch (Exception e) {
      LOG.warn("Failed to close kafka consumer cleanly because of an exception.", e);
    }

    // We don't need to explicitly close the chunkManager, as it will attempt to close itself and
    // will persist the appropriate offset into ZK if it can complete an upload in time

    LOG.info("Astra indexer is closed.");
  }

  @Override
  protected String serviceName() {
    return "astraIndexerService";
  }
}
