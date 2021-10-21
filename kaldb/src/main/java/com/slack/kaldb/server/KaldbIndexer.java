package com.slack.kaldb.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.writer.LogMessageTransformer;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.kaldb.writer.kafka.KaldbKafkaWriter;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KaldbIndexer sets up an indexer that indexes the log messages.
 *
 * <p>This indexer should be testable via junit tests. So, it should have the least number of deps
 * in it's constructor.
 *
 * <p>Single Binary for all Kaldb configured via command line flags.
 *
 * <p>Design should be extensible so we can run as separate components or all components in a single
 * binary.
 */
public class KaldbIndexer extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbIndexer.class);

  @VisibleForTesting
  public static final Map<String, LogMessageTransformer> dataTransformerMap =
      ImmutableMap.of(
          "api_log",
          LogMessageWriterImpl.apiLogTransformer,
          "spans",
          LogMessageWriterImpl.spanTransformer,
          "json",
          LogMessageWriterImpl.jsonLogMessageTransformer);

  private final KaldbKafkaWriter kafkaWriter;

  public IndexingChunkManager<LogMessage> getChunkManager() {
    return chunkManager;
  }

  private final IndexingChunkManager<LogMessage> chunkManager;

  public static LogMessageTransformer getLogMessageTransformer() {
    String dataTransformerConfig = KaldbConfig.get().getIndexerConfig().getDataTransformer();
    if (dataTransformerConfig.isEmpty()) {
      throw new RuntimeException("IndexerConfig can't have an empty dataTransformer config.");
    }

    LogMessageTransformer messageTransformer =
        KaldbIndexer.dataTransformerMap.get(dataTransformerConfig);
    if (messageTransformer == null) {
      throw new RuntimeException("Invalid data transformer config: " + dataTransformerConfig);
    }
    return messageTransformer;
  }

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
  public KaldbIndexer(IndexingChunkManager<LogMessage> chunkManager, KaldbKafkaWriter kafkaWriter) {
    checkNotNull(chunkManager, "Chunk manager can't be null");
    this.chunkManager = chunkManager;
    this.kafkaWriter = kafkaWriter;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting indexing into Kaldb.");
    kafkaWriter.awaitRunning(DEFAULT_START_STOP_DURATION);
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
    kafkaWriter.stopAsync();
    try {
      LOG.info("Waiting for Kafka consumer to close.");
      // Use a more configurable timeout value.
      kafkaWriter.awaitTerminated(2, TimeUnit.SECONDS);
      if (!kafkaWriter.isRunning()) {
        LOG.info("Closed Kafka consumer cleanly");
      } else {
        LOG.warn("Kafka consumer was not closed cleanly");
      }
    } catch (TimeoutException e) {
      LOG.warn("Failed to close kafka consumer cleanly because of a timeout.", e);
    } catch (Exception e) {
      LOG.warn("Failed to close kafka consumer cleanly because of an exception.", e);
    }

    chunkManager.stopAsync();
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);

    LOG.info("Kaldb indexer is closed.");
  }

  @Override
  protected String serviceName() {
    return "kaldbIndexerService";
  }
}
