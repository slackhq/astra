package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.server.KaldbConfig.CHUNK_DATA_PREFIX;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_ROLLOVER_FUTURE_TIMEOUT_MS;
import static com.slack.kaldb.util.ArgValidationUtils.ensureNonNullString;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.*;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.chunk.*;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogStore;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chunk manager implementation that supports appending messages to open chunks. This also is
 * responsible for cleanly transitioning from a full chunk to a new chunk, and uploading that
 * contents to S3, and notifying ZK of state changes.
 *
 * <p>All chunks except one is considered active. The chunk manager writes the message to the
 * currently active chunk. Once a chunk reaches a roll over point(defined by a roll over strategy),
 * the current chunk is marked as read only. At that point a new chunk is created which becomes the
 * active chunk.
 */
public class RecoveryChunkManager<T> {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryChunkManager.class);

  protected final List<Chunk<T>> chunkList = new CopyOnWriteArrayList<>();

  private final File dataDirectory;

  private final String chunkDataPrefix;

  private final BlobFs blobFs;
  private final String s3Bucket;
  private final ChunkRollOverStrategy chunkRollOverStrategy;
  private final MetadataStore metadataStore;
  private final SearchContext searchContext;
  private final KaldbConfigs.IndexerConfig indexerConfig;
  private boolean stopIngestion;
  private ReadWriteChunk<T> activeChunk;

  private final MeterRegistry meterRegistry;
  private final AtomicLong liveMessagesIndexedGauge;
  private final AtomicLong liveBytesIndexedGauge;

  public static final String LIVE_MESSAGES_INDEXED = "live_messages_indexed";
  public static final String LIVE_BYTES_INDEXED = "live_bytes_indexed";
  private AtomicInteger successCounter = new AtomicInteger(0);
  private Phaser rolloverPhaser = new Phaser();

  // fields related to roll over
  private final ListeningExecutorService rolloverExecutorService;
  private final long rolloverFutureTimeoutMs;
  private ListenableFuture<Boolean> rolloverFuture;

  /** Declare all the data stores used by Chunk manager here. */
  private SnapshotMetadataStore snapshotMetadataStore;

  private SearchMetadataStore searchMetadataStore;

  @VisibleForTesting
  public List<Chunk<T>> getChunkList() {
    return chunkList;
  }

  /**
   * For capacity planning, we want to control how many roll overs are in progress at the same time.
   * For recovery for now we only expect at most 1-2 chunks per recovery task. So, a single threaded
   * executor will be sufficient to upload the data in sequence. If we have multiple chunks waiting
   * and upload is the bottleneck, consider multiple threads in the thread pool. Unlike the indexer,
   * we expect the recovery task to ingest a lot more data in parallel. So, there may be some chunks
   * waiting.
   */
  public static ListeningExecutorService makeDefaultRecoveryRollOverExecutor() {
    return MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
  }

  public RecoveryChunkManager(
      String chunkDataPrefix,
      String dataDirectory,
      ChunkRollOverStrategy chunkRollOverStrategy,
      MeterRegistry registry,
      BlobFs blobFs,
      String s3Bucket,
      ListeningExecutorService rollOverExecutorService,
      long rollOverFutureTimeoutMs,
      MetadataStore metadataStore,
      SearchContext searchContext,
      KaldbConfigs.IndexerConfig indexerConfig)
      throws Exception {

    ensureNonNullString(dataDirectory, "The data directory shouldn't be empty");
    this.dataDirectory = new File(dataDirectory);
    this.chunkDataPrefix = chunkDataPrefix;
    this.chunkRollOverStrategy = chunkRollOverStrategy;
    this.meterRegistry = registry;

    // TODO: Pass in id of index in LuceneIndexStore to track this info.
    liveMessagesIndexedGauge = registry.gauge(LIVE_MESSAGES_INDEXED, new AtomicLong(0));
    liveBytesIndexedGauge = registry.gauge(LIVE_BYTES_INDEXED, new AtomicLong(0));

    this.blobFs = blobFs;
    this.s3Bucket = s3Bucket;
    this.rolloverExecutorService = rollOverExecutorService;
    this.rolloverFuture = null;
    this.rolloverFutureTimeoutMs = rollOverFutureTimeoutMs;
    this.metadataStore = metadataStore;
    this.searchContext = searchContext;
    this.indexerConfig = indexerConfig;
    snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    searchMetadataStore = new SearchMetadataStore(metadataStore, false);
    this.stopIngestion = false;
    // Register the phaser on constructor and also for every roll over. Wait for phaser in close().
    rolloverPhaser.register();

    activeChunk = null;

    LOG.info(
        "Created a chunk manager with prefix {} and dataDirectory {}",
        chunkDataPrefix,
        dataDirectory);
  }

  /**
   * This function ingests a message into a chunk in the chunk manager. It performs the following
   * steps: 1. Find an active chunk. 2. Ingest the message into the active chunk. 3. Calls the
   * shouldRollOver function to check if the chunk is full. 4. If the chunk is full, queue the
   * active chunk for roll over.
   */
  public void addMessage(final T message, long msgSize, String kafkaPartitionId, long offset)
      throws IOException {
    if (stopIngestion) {
      // Currently, this flag is set on only a chunkRollOverException.
      LOG.warn("Stopping ingestion due to a chunk roll over exception.");
      throw new ChunkRollOverException("Stopping ingestion due to chunk roll over exception.");
    }

    // find the active chunk and add a message to it
    ReadWriteChunk<T> currentChunk = getOrCreateActiveChunk(kafkaPartitionId, indexerConfig);
    currentChunk.addMessage(message, kafkaPartitionId, offset);
    long currentIndexedMessages = liveMessagesIndexedGauge.incrementAndGet();
    long currentIndexedBytes = liveBytesIndexedGauge.addAndGet(msgSize);

    // If active chunk is full roll it over.
    if (chunkRollOverStrategy.shouldRollOver(currentIndexedBytes, currentIndexedMessages)) {
      LOG.info(
          "After {} messages and {} bytes rolling over chunk {}.",
          currentIndexedMessages,
          currentIndexedBytes,
          currentChunk.id());
      doRollover(currentChunk);
    }
  }

  /**
   * This method initiates a roll over of the active chunk. In future, consider moving the some of
   * the roll over logic into ChunkImpl.
   */
  private void doRollover(ReadWriteChunk<T> currentChunk) {
    // Set activeChunk to null first, so we can initiate the roll over.
    activeChunk = null;
    liveBytesIndexedGauge.set(0);
    liveMessagesIndexedGauge.set(0);
    // Set the end time of the chunk and start the roll over.
    currentChunk.info().setChunkLastUpdatedTimeEpochMs(Instant.now().toEpochMilli());

    RollOverChunkTask<T> rollOverChunkTask =
        new RollOverChunkTask<>(
            currentChunk, meterRegistry, blobFs, s3Bucket, currentChunk.info().chunkId);

    if ((rolloverFuture == null) || rolloverFuture.isDone()) {
      rolloverFuture = rolloverExecutorService.submit(rollOverChunkTask);
      rolloverPhaser.register();

      Futures.addCallback(
          rolloverFuture,
          new FutureCallback<>() {
            @Override
            public void onSuccess(Boolean success) {
              if (success == null || !success) {
                LOG.warn("Roll over failed");
                stopIngestion = true;
              } else {
                successCounter.incrementAndGet();
              }
              rolloverPhaser.arrive();
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.warn("Roll over failed with an exception", t);
              stopIngestion = true;
              rolloverPhaser.arrive();
            }
          },
          MoreExecutors.directExecutor());
    } else {
      throw new ChunkRollOverException(
          String.format(
              "The chunk roll over %s is already in progress."
                  + "It is not recommended to index faster than we can roll over, since we may not be able to keep up",
              currentChunk.info()));
    }
  }

  /*
   * When the ChunkManager is being closed, roll over active chunk so we can be sure that it's data is persisted in
   * a remote store.
   */
  public void rollOverActiveChunk() {
    LOG.info("Rolling over active chunk");
    doRollover(getActiveChunk());
  }

  @VisibleForTesting
  public ReadWriteChunk<T> getActiveChunk() {
    return activeChunk;
  }

  /**
   * getChunk returns the active chunk. If no chunk is active because of roll over or this is the
   * first message, create one chunk and set is as active.
   *
   * <p>NOTE: Currently, this logic assumes that we are indexing live data. So, the startTime of the
   * data in the chunk is set as system time. However, this assumption may not be true always. In
   * future, set the start time of the chunk based on the timestamp from the message.
   */
  private ReadWriteChunk<T> getOrCreateActiveChunk(
      String kafkaPartitionId, KaldbConfigs.IndexerConfig indexerConfig) throws IOException {
    if (activeChunk == null) {
      @SuppressWarnings("unchecked")
      LogStore<T> logStore =
          (LogStore<T>)
              LuceneIndexStoreImpl.makeLogStore(
                  dataDirectory, indexerConfig.getLuceneConfig(), meterRegistry);

      ReadWriteChunk<T> newChunk =
          new RecoveryChunkImpl<T>(
              logStore,
              chunkDataPrefix,
              meterRegistry,
              searchMetadataStore,
              snapshotMetadataStore,
              searchContext,
              kafkaPartitionId);
      chunkList.add(newChunk);
      // Register the chunk, so we can search it.
      newChunk.postCreate();
      activeChunk = newChunk;
    }
    return activeChunk;
  }

  @VisibleForTesting
  public ListenableFuture<?> getRolloverFuture() {
    return rolloverFuture;
  }

  /**
   * Close the chunk manager safely by finishing all the pending roll overs and closing chunks
   * cleanly. To ensure data integrity don't throw exceptions before chunk close.
   *
   * <p>TODO: When closing a ChunkManager we need to ensure that all the active chunks are closed,
   * and the data is uploaded safely to a remote store. If the active chunks are not closed
   * correctly, we would throw away indexed data and would need to index the same data again.
   *
   * <p>TODO: Consider implementing async close. Also, stop new writes once close is called.
   */
  public void close() throws IOException, InterruptedException, TimeoutException {
    LOG.info("Closing recovery chunk manager.");

    // Roll over active chunk.
    if (activeChunk != null) {
      rollOverActiveChunk();
    }

    // Wait for all the roll overs to complete.
    // TODO: Move timeout into a constant.
    rolloverPhaser.awaitAdvanceInterruptibly(rolloverPhaser.arrive(), 10, TimeUnit.MINUTES);

    // Stop executor service from taking on new tasks.
    rolloverExecutorService.shutdown();

    // Close roll over executor service.
    try {
      // A short timeout here is fine here since there are no more tasks.
      rolloverExecutorService.awaitTermination(1, TimeUnit.SECONDS);
      rolloverExecutorService.shutdownNow();
    } catch (InterruptedException e) {
      LOG.warn("Encountered error shutting down roll over executor.", e);
    }

    // Close all chunks.
    for (Chunk<T> chunk : chunkList) {
      try {
        chunk.close();
      } catch (IOException e) {
        LOG.error("Failed to close chunk.", e);
      }
    }

    searchMetadataStore.close();
    snapshotMetadataStore.close();
    LOG.info("Closed recovery chunk manager.");
  }

  public static RecoveryChunkManager<LogMessage> fromConfig(
      MeterRegistry meterRegistry,
      MetadataStore metadataStore,
      KaldbConfigs.IndexerConfig indexerConfig,
      BlobFs blobFs,
      KaldbConfigs.S3Config s3Config)
      throws Exception {

    ChunkRollOverStrategy chunkRollOverStrategy =
        ChunkRollOverStrategyImpl.fromConfig(indexerConfig);

    return new RecoveryChunkManager<>(
        CHUNK_DATA_PREFIX,
        indexerConfig.getDataDirectory(),
        chunkRollOverStrategy,
        meterRegistry,
        blobFs,
        s3Config.getS3Bucket(),
        makeDefaultRecoveryRollOverExecutor(),
        DEFAULT_ROLLOVER_FUTURE_TIMEOUT_MS,
        metadataStore,
        SearchContext.fromConfig(indexerConfig.getServerConfig()),
        indexerConfig);
  }

  @VisibleForTesting
  public SnapshotMetadataStore getSnapshotMetadataStore() {
    return snapshotMetadataStore;
  }
}
