package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.server.KaldbConfig.CHUNK_DATA_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.*;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.chunk.*;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single chunk manager manages a single chunk of data. The addMessage API adds a message to the
 * chunk without rollover. The rollOver API sets the chunk to read only and rolls over the chunk.
 * The close call performs clean up operations and closes the chunk.
 */
public class RecoveryChunkManager<T> extends ChunkManagerBase<T> {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryChunkManager.class);
  // This field controls the maximum amount of time we wait for a rollover to complete.
  private static final int MAX_ROLLOVER_MINUTES = 10;

  private final ChunkFactory<T> recoveryChunkFactory;
  private final ChunkRolloverFactory chunkRolloverFactory;
  private boolean readOnly;
  private ReadWriteChunk<T> activeChunk;

  private final AtomicLong liveMessagesIndexedGauge;
  private final AtomicLong liveBytesIndexedGauge;

  public static final String LIVE_MESSAGES_INDEXED = "live_messages_indexed";
  public static final String LIVE_BYTES_INDEXED = "live_bytes_indexed";

  // fields related to roll over
  private final ListeningExecutorService rolloverExecutorService;
  private boolean rollOverFailed;

  public RecoveryChunkManager(
      ChunkFactory<T> recoveryChunkFactory,
      ChunkRolloverFactory chunkRolloverFactory,
      MeterRegistry registry) {

    // TODO: Pass in id of index in LuceneIndexStore to track this info.
    liveMessagesIndexedGauge = registry.gauge(LIVE_MESSAGES_INDEXED, new AtomicLong(0));
    liveBytesIndexedGauge = registry.gauge(LIVE_BYTES_INDEXED, new AtomicLong(0));
    this.recoveryChunkFactory = recoveryChunkFactory;
    this.chunkRolloverFactory = chunkRolloverFactory;

    this.rolloverExecutorService =
        MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
    this.rollOverFailed = false;

    activeChunk = null;

    LOG.info("Created a recovery chunk manager");
  }

  public void addMessage(final T message, long msgSize, String kafkaPartitionId, long offset)
      throws IOException {
    if (readOnly) {
      // We stop ingestion on chunk roll over failures or if the chunk manager is shutting down.
      LOG.warn("Ingestion is stopped since the chunk is in read only mode.");
      throw new IllegalStateException("Ingestion is stopped since chunk is read only.");
    }

    // find the active chunk and add a message to it
    ReadWriteChunk<T> currentChunk = getOrCreateActiveChunk(kafkaPartitionId);
    currentChunk.addMessage(message, kafkaPartitionId, offset);
    liveMessagesIndexedGauge.incrementAndGet();
    liveBytesIndexedGauge.addAndGet(msgSize);
  }

  /** This method initiates a roll over of the active chunk. */
  private void doRollover(ReadWriteChunk<T> currentChunk) {
    // Set activeChunk to null first, so we can initiate the roll over.
    activeChunk = null;
    liveBytesIndexedGauge.set(0);
    liveMessagesIndexedGauge.set(0);
    // Set the end time of the chunk and start the roll over.
    currentChunk.info().setChunkLastUpdatedTimeEpochMs(Instant.now().toEpochMilli());

    RollOverChunkTask<T> rollOverChunkTask =
        chunkRolloverFactory.getRollOverChunkTask(currentChunk, currentChunk.info().chunkId);

    ListenableFuture<Boolean> rolloverFuture = rolloverExecutorService.submit(rollOverChunkTask);
    Futures.addCallback(
        rolloverFuture,
        new FutureCallback<>() {
          @Override
          public void onSuccess(Boolean success) {
            if (success == null || !success) {
              LOG.warn("Roll over failed");
              rollOverFailed = true;
            }
          }

          @Override
          public void onFailure(Throwable t) {
            LOG.warn("Roll over failed with an exception", t);
            rollOverFailed = true;
          }
        },
        MoreExecutors.directExecutor());
  }

  /**
   * getChunk returns the active chunk. If no chunk is active because of roll over or this is the
   * first message, create one chunk and set is as active.
   */
  private ReadWriteChunk<T> getOrCreateActiveChunk(String kafkaPartitionId) throws IOException {
    if (activeChunk == null) {
      recoveryChunkFactory.setKafkaPartitionId(kafkaPartitionId);
      ReadWriteChunk<T> newChunk = recoveryChunkFactory.makeChunk();
      chunkList.add(newChunk);
      // Run post create actions on the chunk.
      newChunk.postCreate();
      activeChunk = newChunk;
    }
    return activeChunk;
  }

  // The callers need to wait for rollovers to complete and the status of the rollovers. So, we
  // expose this function to wait for rollovers and report their status.
  // We don't call this function during shutdown, so callers should call this function before close.
  public boolean waitForRollOvers() {
    LOG.info("Waiting for rollovers to complete");
    // Stop accepting new writes to the chunks.
    readOnly = true;

    // Roll over active chunk.
    if (activeChunk != null) {
      doRollover(activeChunk);
    }

    // Stop executor service from taking on new tasks.
    rolloverExecutorService.shutdown();

    // Close roll over executor service.
    try {
      // A short timeout here is fine here since there are no more tasks.
      rolloverExecutorService.awaitTermination(MAX_ROLLOVER_MINUTES, TimeUnit.MINUTES);
      rolloverExecutorService.shutdownNow();
    } catch (InterruptedException e) {
      LOG.warn("Encountered error shutting down roll over executor.", e);
      return false;
    }

    if (rollOverFailed) {
      LOG.info("Rollover has failed.");
      return false;
    } else {
      LOG.info("Rollover is completed");
      return true;
    }
  }

  @Override
  protected void startUp() throws Exception {}

  /**
   * Close the chunks and shut down the chunk manager. To ensure that the chunks are rolled over
   * call `waitForRollovers` before the chunk manager is closed. This ensures that no data is lost.
   */
  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing recovery chunk manager.");

    readOnly = true;

    // Close all chunks.
    for (Chunk<T> chunk : chunkList) {
      try {
        chunk.close();
      } catch (IOException e) {
        LOG.error("Failed to close chunk.", e);
      }
    }

    LOG.info("Closed recovery chunk manager.");
  }

  @VisibleForTesting
  public ReadWriteChunk<T> getActiveChunk() {
    return activeChunk;
  }

  public static RecoveryChunkManager<LogMessage> fromConfig(
      MeterRegistry meterRegistry,
      MetadataStore metadataStore,
      KaldbConfigs.IndexerConfig indexerConfig,
      BlobFs blobFs,
      KaldbConfigs.S3Config s3Config)
      throws Exception {

    // TODO: Pass these metadata stores in and close them correctly.
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(metadataStore, false);
    SearchContext searchContext = SearchContext.fromConfig(indexerConfig.getServerConfig());

    RecoveryChunkFactoryImpl<LogMessage> recoveryChunkFactory =
        new RecoveryChunkFactoryImpl<>(
            indexerConfig,
            CHUNK_DATA_PREFIX,
            meterRegistry,
            searchMetadataStore,
            snapshotMetadataStore,
            searchContext);

    ChunkRolloverFactory chunkRolloverFactory =
        new ChunkRolloverFactory(
            new NeverRolloverChunkStrategyImpl(), blobFs, s3Config.getS3Bucket(), meterRegistry);

    return new RecoveryChunkManager<>(recoveryChunkFactory, chunkRolloverFactory, meterRegistry);
  }
}
