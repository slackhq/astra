package com.slack.kaldb.chunk.manager;

import static com.slack.kaldb.util.ArgValidationUtils.ensureNonNullString;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.Chunk;
import com.slack.kaldb.chunk.ReadWriteChunkImpl;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogStore;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
public class IndexingChunkManager<T> extends ChunkManager<T> {
  private static final Logger LOG = LoggerFactory.getLogger(IndexingChunkManager.class);
  public static final long DEFAULT_ROLLOVER_FUTURE_TIMEOUT_MS = 30000;

  private final File dataDirectory;

  // TODO: ChunkDataPrefix can be moved to KaldbConfig?
  private final String chunkDataPrefix;

  // TODO: Pass a reference to BlobFS instead of S3BlobFS.
  private final S3BlobFs s3BlobFs;
  private final String s3Bucket;
  private final ChunkRollOverStrategy chunkRollOverStrategy;
  private final MetadataStoreService metadataStoreService;
  private final SearchContext searchContext;
  private ReadWriteChunkImpl<T> activeChunk;

  private final MeterRegistry meterRegistry;
  private final AtomicLong liveMessagesIndexedGauge;
  private final AtomicLong liveBytesIndexedGauge;

  public static final String LIVE_MESSAGES_INDEXED = "live_messages_indexed";
  public static final String LIVE_BYTES_INDEXED = "live_bytes_indexed";

  // fields related to roll over
  private final ListeningExecutorService rolloverExecutorService;
  private final long rolloverFutureTimeoutMs;
  private ListenableFuture<Boolean> rolloverFuture;

  // TODO: Pass this in via config file.
  private static final String CHUNK_DATA_PREFIX = "log_data_";

  /**
   * A flag to indicate that ingestion should be stopped. Currently, we only stop ingestion when a
   * chunk roll over fails. Ideally, access to this field should be synchronized. But we don't
   * synchronize on this field for 2 reasons: (a) This field is checked for every message so it's in
   * hot path. (b) This field is only set once in the same thread and then entire VM stops
   * afterwards.
   */
  private boolean stopIngestion;

  /** Declare all the data stores used by Chunk manager here. */
  private SnapshotMetadataStore snapshotMetadataStore;

  private SearchMetadataStore searchMetadataStore;

  /**
   * For capacity planning, we want to control how many roll overs are in progress at the same time.
   * To maintain flexibility on the final policy, we will implement it using a ThreadPoolExecutor.
   * For now, we only want 1 roll over in process at any time and abort the indexer if a second roll
   * over is in process, since otherwise we will never be able to keep up. So, we create a custom
   * roll over executor that can only execute one roll over task at a time and throws a
   * RejectedExecutionHandler exception when a second one is called (the default policy).
   */
  public static ListeningExecutorService makeDefaultRollOverExecutor() {
    // TODO: Create a named thread pool and pass it in.
    ThreadPoolExecutor rollOverExecutor =
        new ThreadPoolExecutor(
            1, 1, 0, MILLISECONDS, new SynchronousQueue<>(), new ThreadPoolExecutor.AbortPolicy());
    return MoreExecutors.listeningDecorator(
        MoreExecutors.getExitingExecutorService(rollOverExecutor));
  }

  public IndexingChunkManager(
      String chunkDataPrefix,
      String dataDirectory,
      ChunkRollOverStrategy chunkRollOverStrategy,
      MeterRegistry registry,
      S3BlobFs s3BlobFs,
      String s3Bucket,
      ListeningExecutorService rollOverExecutorService,
      long rollOverFutureTimeoutMs,
      MetadataStoreService metadataStoreService,
      SearchContext searchContext) {

    ensureNonNullString(dataDirectory, "The data directory shouldn't be empty");
    this.dataDirectory = new File(dataDirectory);
    this.chunkDataPrefix = chunkDataPrefix;
    this.chunkRollOverStrategy = chunkRollOverStrategy;
    this.meterRegistry = registry;

    // TODO: Pass in id of index in LuceneIndexStore to track this info.
    liveMessagesIndexedGauge = registry.gauge(LIVE_MESSAGES_INDEXED, new AtomicLong(0));
    liveBytesIndexedGauge = registry.gauge(LIVE_BYTES_INDEXED, new AtomicLong(0));

    this.s3BlobFs = s3BlobFs;
    this.s3Bucket = s3Bucket;
    this.rolloverExecutorService = rollOverExecutorService;
    this.rolloverFuture = null;
    this.rolloverFutureTimeoutMs = rollOverFutureTimeoutMs;
    this.metadataStoreService = metadataStoreService;
    this.searchContext = searchContext;
    stopIngestion = true;
    activeChunk = null;

    LOG.info(
        "Created a chunk manager with prefix {} and dataDirectory {}",
        chunkDataPrefix,
        dataDirectory);
  }

  /**
   * This function ingests a message into a chunk in the chunk manager. It performs the following
   * steps: 1. Find an active chunk. 2. Ingest the message into the active chunk. 3. Calls the
   * shouldRollOver function to check if the chunk is full. 4. If the chunk is full, initiate the
   * roll over of the active chunk.
   *
   * <p>We assume that there is a single chunk manager per process and only one thread is writing to
   * this class. We allow several readers though.
   *
   * @param message Message to be ingested
   * @param msgSize Serialized size of raw message in bytes.
   * @param offset Kafka offset of the message.
   *     <p>TODO: Indexer should stop cleanly if the roll over fails or an exception.
   *     <p>TODO: Delete the snapshot from local disk once it is replicated elsewhere after X min.
   */
  public void addMessage(final T message, long msgSize, long offset) throws IOException {
    if (stopIngestion) {
      // Currently, this flag is set on only a chunkRollOverException.
      LOG.warn("Stopping ingestion due to a chunk roll over exception.");
      throw new ChunkRollOverException("Stopping ingestion due to chunk roll over exception.");
    }

    // find the active chunk and add a message to it
    ReadWriteChunkImpl<T> currentChunk = getOrCreateActiveChunk();
    currentChunk.addMessage(message);
    long currentIndexedMessages = liveMessagesIndexedGauge.incrementAndGet();
    long currentIndexedBytes = liveBytesIndexedGauge.addAndGet(msgSize);

    // If active chunk is full roll it over.
    if (chunkRollOverStrategy.shouldRollOver(currentIndexedBytes, currentIndexedMessages)) {
      LOG.info(
          "After {} messages and {} bytes rolling over chunk {}.",
          currentIndexedMessages,
          currentIndexedBytes,
          currentChunk.id());
      currentChunk.info().setNumDocs(currentIndexedMessages);
      currentChunk.info().setChunkSize(currentIndexedBytes);
      doRollover(currentChunk);
    }
  }

  /**
   * This method initiates a roll over of the active chunk. In future, consider moving the some of
   * the roll over logic into ChunkImpl.
   */
  private void doRollover(ReadWriteChunkImpl<T> currentChunk) {
    // TODO: Register non-live snapshot, add new search node for snapshot, Remove live
    //  snapshot,  remove search node for live.

    // Set activeChunk to null first, so we can initiate the roll over.
    activeChunk = null;
    liveBytesIndexedGauge.set(0);
    liveMessagesIndexedGauge.set(0);
    // Set the end time of the chunk and start the roll over.
    currentChunk.info().setChunkLastUpdatedTimeEpochMs(Instant.now().toEpochMilli());

    RollOverChunkTask<T> rollOverChunkTask =
        new RollOverChunkTask<T>(
            currentChunk, meterRegistry, s3BlobFs, s3Bucket, currentChunk.info().chunkId);

    if ((rolloverFuture == null) || rolloverFuture.isDone()) {
      rolloverFuture = rolloverExecutorService.submit(rollOverChunkTask);
      Futures.addCallback(
          rolloverFuture,
          new FutureCallback<>() {
            @Override
            public void onSuccess(Boolean success) {
              if (success == null || !success) {
                stopIngestion = true;
              }
            }

            @Override
            public void onFailure(Throwable t) {
              stopIngestion = true;
            }
          },
          MoreExecutors.directExecutor());
    } else {
      throw new ChunkRollOverInProgressException(
          String.format(
              "The chunk roll over %s is already in progress."
                  + "It is not recommended to index faster than we can roll over, since we may not be able to keep up.",
              currentChunk.info()));
    }
  }

  /*
   * When the ChunkManager is being closed, roll over active chunk so we can be sure that it's data is persisted in
   * a remote store.
   */
  @VisibleForTesting
  public void rollOverActiveChunk() {
    LOG.info("Rolling over active chunk");
    doRollover(getActiveChunk());
  }

  @VisibleForTesting
  public ReadWriteChunkImpl<T> getActiveChunk() {
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
  private ReadWriteChunkImpl<T> getOrCreateActiveChunk() throws IOException {
    if (activeChunk == null) {
      // TODO: Rewrite makeLogStore to not read from kaldb config after initialization since it
      //  complicates unit tests.
      @SuppressWarnings("unchecked")
      LogStore<T> logStore =
          (LogStore<T>) LuceneIndexStoreImpl.makeLogStore(dataDirectory, meterRegistry);

      ReadWriteChunkImpl<T> newChunk =
          new ReadWriteChunkImpl<>(logStore, chunkDataPrefix, meterRegistry);
      chunkList.add(newChunk);

      // TODO: Register live snapshot, register a search metadata node.

      activeChunk = newChunk;
    }
    return activeChunk;
  }

  public void removeStaleChunks(List<Chunk<T>> staleChunks) {
    if (staleChunks.isEmpty()) return;

    LOG.info("Stale chunks to be removed are: {}", staleChunks);

    if (chunkList.isEmpty()) {
      LOG.warn("Possible race condition, there are no chunks in chunkList");
    }

    staleChunks.forEach(
        chunk -> {
          try {
            if (chunkList.contains(chunk)) {
              String chunkInfo = chunk.info().toString();
              LOG.info("Deleting chunk {}.", chunkInfo);

              // TODO: Remove search node.

              // Remove the chunk first from the map so we don't search it anymore.
              // Note that any pending queries may still hold references to these chunks
              chunkList.remove(chunk);

              chunk.close();
              LOG.info("Deleted and cleaned up chunk {}.", chunkInfo);
            } else {
              LOG.warn(
                  "Possible bug or race condition! Chunk {} doesn't exist in chunk list {}.",
                  chunk,
                  chunkList);
            }
          } catch (Exception e) {
            LOG.warn("Exception when deleting chunk", e);
          }
        });
  }

  @VisibleForTesting
  public ListenableFuture<?> getRolloverFuture() {
    return rolloverFuture;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting indexing chunk manager");
    metadataStoreService.awaitRunning(KaldbConfig.DEFAULT_START_STOP_DURATION);

    searchMetadataStore =
        new SearchMetadataStore(
            metadataStoreService.getMetadataStore(),
            KaldbConfig.SEARCH_METADATA_STORE_ZK_PATH,
            false);

    snapshotMetadataStore =
        new SnapshotMetadataStore(
            metadataStoreService.getMetadataStore(),
            KaldbConfig.SNAPSHOT_METADATA_STORE_ZK_PATH,
            false);

    // TODO: Move this registration closer to chunk metadata
    SearchMetadata searchMetadata =
        toSearchMetadata(SearchMetadata.LIVE_SNAPSHOT_NAME, searchContext);
    searchMetadataStore.createSync(searchMetadata);

    // todo - we should reconsider what it means to be initialized, vs running
    // todo - potentially defer threadpool creation until the startup has been called?
    // prevents use of chunk manager until the service has started
    stopIngestion = false;
  }

  private SearchMetadata toSearchMetadata(String snapshotName, SearchContext searchContext) {
    return new SearchMetadata(searchContext.hostname, snapshotName, searchContext.toUrl());
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
  @Override
  protected void shutDown() throws IOException {
    LOG.info("Closing indexing chunk manager.");

    // Stop executor service from taking on new tasks.
    rolloverExecutorService.shutdown();

    // Finish existing rollovers.
    if (rolloverFuture != null && !rolloverFuture.isDone()) {
      try {
        LOG.info("Waiting for roll over to complete before closing..");
        rolloverFuture.get(rolloverFutureTimeoutMs, MILLISECONDS);
        LOG.info("Roll over completed successfully. Closing rollover task.");
      } catch (Exception e) {
        LOG.warn("Roll over failed with Exception", e);
        // TODO: Throw a roll over failed exception and stop the indexer.
      }
    } else {
      LOG.info("Roll over future completed successfully.");
    }

    // Close roll over executor service.
    try {
      // A short timeout here is fine here since there are no more tasks.
      rolloverExecutorService.awaitTermination(1, TimeUnit.SECONDS);
      rolloverExecutorService.shutdownNow();
    } catch (InterruptedException e) {
      LOG.warn("Encountered error shutting down roll over executor.", e);
    }

    for (Chunk<T> chunk : chunkList) {
      try {
        chunk.close();
      } catch (IOException e) {
        LOG.error("Failed to close chunk.", e);
      }
    }

    searchMetadataStore.close();
    snapshotMetadataStore.close();
    LOG.info("Closed indexing chunk manager.");
  }

  public static IndexingChunkManager<LogMessage> fromConfig(
      MeterRegistry meterRegistry,
      MetadataStoreService metadataStoreService,
      KaldbConfigs.ServerConfig serverConfig) {
    ChunkRollOverStrategy chunkRollOverStrategy = ChunkRollOverStrategyImpl.fromConfig();

    // TODO: Read the config values for chunk manager from config file.
    return new IndexingChunkManager<>(
        CHUNK_DATA_PREFIX,
        KaldbConfig.get().getIndexerConfig().getDataDirectory(),
        chunkRollOverStrategy,
        meterRegistry,
        getS3BlobFsClient(KaldbConfig.get()),
        KaldbConfig.get().getS3Config().getS3Bucket(),
        makeDefaultRollOverExecutor(),
        DEFAULT_ROLLOVER_FUTURE_TIMEOUT_MS,
        metadataStoreService,
        SearchContext.fromConfig(serverConfig));
  }

  @VisibleForTesting
  public SnapshotMetadataStore getSnapshotMetadataStore() {
    return snapshotMetadataStore;
  }

  @VisibleForTesting
  public SearchMetadataStore getSearchMetadataStore() {
    return searchMetadataStore;
  }
}
