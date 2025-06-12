package com.slack.astra.chunkManager;

import static com.slack.astra.server.AstraConfig.CHUNK_DATA_PREFIX;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.util.ArgValidationUtils.ensureNonNullString;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.chunk.Chunk;
import com.slack.astra.chunk.ChunkInfo;
import com.slack.astra.chunk.IndexingChunkImpl;
import com.slack.astra.chunk.ReadWriteChunk;
import com.slack.astra.chunk.SearchContext;
import com.slack.astra.chunkrollover.ChunkRollOverStrategy;
import com.slack.astra.chunkrollover.DiskOrMessageCountBasedRolloverStrategy;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogStore;
import com.slack.astra.logstore.LuceneIndexStoreImpl;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.curator.x.async.AsyncCuratorFramework;
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
public class IndexingChunkManager<T> extends ChunkManagerBase<T> {
  private static final Logger LOG = LoggerFactory.getLogger(IndexingChunkManager.class);

  private final File dataDirectory;

  private final String chunkDataPrefix;

  private final BlobStore blobStore;
  private final ChunkRollOverStrategy chunkRollOverStrategy;
  private final AsyncCuratorFramework curatorFramework;
  private final SearchContext searchContext;
  private final AstraConfigs.IndexerConfig indexerConfig;
  private final AstraConfigs.MetadataStoreConfig metadataStoreConfig;
  private ReadWriteChunk<T> activeChunk;

  private final MeterRegistry meterRegistry;
  private final AtomicLong liveMessagesIndexedGauge;
  private final AtomicLong liveBytesIndexedGauge;

  public static final String LIVE_MESSAGES_INDEXED = "live_messages_indexed";
  public static final String LIVE_BYTES_INDEXED = "live_bytes_indexed";

  // fields related to roll over
  private final ListeningExecutorService rolloverExecutorService;

  private ListenableFuture<Boolean> rolloverFuture;

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
  @SuppressWarnings("UnstableApiUsage")
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
      BlobStore blobStore,
      ListeningExecutorService rolloverExecutorService,
      AsyncCuratorFramework curatorFramework,
      SearchContext searchContext,
      AstraConfigs.IndexerConfig indexerConfig,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig) {

    ensureNonNullString(dataDirectory, "The data directory shouldn't be empty");
    this.dataDirectory = new File(dataDirectory);
    this.chunkDataPrefix = chunkDataPrefix;
    this.chunkRollOverStrategy = chunkRollOverStrategy;
    this.meterRegistry = registry;

    // TODO: Pass in id of index in LuceneIndexStore to track this info.
    liveMessagesIndexedGauge = registry.gauge(LIVE_MESSAGES_INDEXED, new AtomicLong(0));
    liveBytesIndexedGauge = registry.gauge(LIVE_BYTES_INDEXED, new AtomicLong(0));

    this.blobStore = blobStore;
    this.rolloverExecutorService = rolloverExecutorService;
    this.rolloverFuture = null;
    this.curatorFramework = curatorFramework;
    this.searchContext = searchContext;
    this.indexerConfig = indexerConfig;
    this.metadataStoreConfig = metadataStoreConfig;

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
   * @param kafkaPartitionId Kafka partition the message is read from.
   * @param offset Kafka offset of the message.
   *     <p>TODO: Indexer should stop cleanly if the roll over fails or an exception.
   */
  @Override
  public void addMessage(
      final Trace.Span message, long msgSize, String kafkaPartitionId, long offset)
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
        new RollOverChunkTask<>(currentChunk, meterRegistry, blobStore);

    if ((rolloverFuture == null) || rolloverFuture.isDone()) {
      rolloverFuture = rolloverExecutorService.submit(rollOverChunkTask);
      Futures.addCallback(
          rolloverFuture,
          new FutureCallback<>() {
            @Override
            public void onSuccess(Boolean success) {
              if (success == null || !success) {
                LOG.error("RollOverChunkTask success=false for chunk={}", currentChunk.info());
                stopIngestion = true;
              }
              deleteStaleData();
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error("Roll over failed with an exception for chunk={}", currentChunk.info(), t);
              stopIngestion = true;
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
    LOG.debug("Rolling over active chunk");
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
      String kafkaPartitionId, AstraConfigs.IndexerConfig indexerConfig) throws IOException {
    if (activeChunk == null) {
      @SuppressWarnings("unchecked")
      LogStore logStore =
          LuceneIndexStoreImpl.makeLogStore(
              dataDirectory, indexerConfig.getLuceneConfig(), meterRegistry);

      chunkRollOverStrategy.setActiveChunkDirectory(logStore.getDirectory());

      ReadWriteChunk<T> newChunk =
          new IndexingChunkImpl<>(
              logStore,
              chunkDataPrefix,
              meterRegistry,
              searchMetadataStore,
              snapshotMetadataStore,
              searchContext,
              kafkaPartitionId);
      chunkMap.put(newChunk.id(), newChunk);
      // Register the chunk, so we can search it.
      newChunk.postCreate();
      activeChunk = newChunk;
    }
    return activeChunk;
  }

  private void deleteStaleData() {
    Duration staleDelayDuration = Duration.ofSeconds(indexerConfig.getStaleDurationSecs());
    int limit = indexerConfig.getMaxChunksOnDisk();

    Instant startInstant = Instant.now();
    final Instant staleCutOffMs = startInstant.minusSeconds(staleDelayDuration.toSeconds());

    // Delete any stale chunks that are either too old, or those chunks that go over the max allowed
    // on
    // any given node
    deleteStaleChunksPastCutOff(staleCutOffMs);
    deleteChunksOverLimit(limit);
  }

  private void deleteChunksOverLimit(int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("limit can't be negative");
    }

    final List<Chunk<T>> unsortedChunks = this.getChunkList();

    if (unsortedChunks.size() <= limit) {
      LOG.info("Unsorted chunks less than or equal to limit. Doing nothing.");
      return;
    }

    // Sorts the list in ascending order (i.e. oldest to newest) and only gets chunks that we've
    // taken a snapshot of
    final List<Chunk<T>> sortedChunks =
        unsortedChunks.stream()
            .sorted(Comparator.comparingLong(chunk -> chunk.info().getChunkCreationTimeEpochMs()))
            .filter(chunk -> chunk.info().getChunkSnapshotTimeEpochMs() > 0)
            .toList();

    final int totalChunksToDelete = sortedChunks.size() - limit;

    final List<Chunk<T>> chunksToDelete = sortedChunks.subList(0, totalChunksToDelete);

    LOG.info("Number of chunks past limit of {} is {}", limit, chunksToDelete.size());
    this.removeStaleChunks(chunksToDelete);
  }

  private void deleteStaleChunksPastCutOff(Instant staleDataCutOffMs) {
    List<Chunk<T>> staleChunks = new ArrayList<>();
    for (Chunk<T> chunk : this.getChunkList()) {
      if (chunkIsStale(chunk.info(), staleDataCutOffMs)) {
        staleChunks.add(chunk);
      }
    }

    LOG.info(
        "Number of stale chunks at staleDataCutOffMs {} is {}",
        staleDataCutOffMs,
        staleChunks.size());
    this.removeStaleChunks(staleChunks);
  }

  private boolean chunkIsStale(ChunkInfo chunkInfo, Instant staleDataCutoffMs) {
    return chunkInfo.getChunkSnapshotTimeEpochMs() > 0
        && chunkInfo.getChunkSnapshotTimeEpochMs() <= staleDataCutoffMs.toEpochMilli();
  }

  private void removeStaleChunks(List<Chunk<T>> staleChunks) {
    if (staleChunks.isEmpty()) return;

    LOG.info("Stale chunks to be removed are: {}", staleChunks);

    if (chunkMap.isEmpty()) {
      LOG.warn("Possible race condition, there are no chunks in chunkList");
    }

    staleChunks.forEach(
        chunk -> {
          try {
            if (chunkMap.containsKey(chunk.id())) {
              String chunkInfo = chunk.info().toString();
              LOG.debug("Deleting chunk {}.", chunkInfo);

              // Remove the chunk first from the map so we don't search it anymore.
              // Note that any pending queries may still hold references to these chunks
              chunkMap.remove(chunk.id(), chunk);

              chunk.close();
              LOG.debug("Deleted and cleaned up chunk {}.", chunkInfo);
            } else {
              LOG.warn(
                  "Possible bug or race condition! Chunk {} doesn't exist in chunk list {}.",
                  chunk,
                  chunkMap.values());
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

    searchMetadataStore =
        new SearchMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, false);
    snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry);

    stopIngestion = false;
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

    chunkRollOverStrategy.close();

    // Stop executor service from taking on new tasks.
    rolloverExecutorService.shutdown();

    // Finish existing rollovers.
    if (rolloverFuture != null && !rolloverFuture.isDone()) {
      try {
        LOG.info("Waiting for roll over to complete before closing..");
        rolloverFuture.get(DEFAULT_START_STOP_DURATION.get(ChronoUnit.SECONDS), TimeUnit.SECONDS);
        LOG.info("Roll over completed successfully. Closing rollover task.");
      } catch (Exception e) {
        LOG.warn("Roll over failed with Exception", e);
        // TODO: Throw a roll over failed exception and stop the indexer.
      }
    } else {
      LOG.info("Roll over future completed successfully.");
    }

    // Forcefully close rollover executor service. There may be a pending rollover, but we have
    // reached the max time.
    rolloverExecutorService.shutdownNow();

    for (Chunk<T> chunk : chunkMap.values()) {
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
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.IndexerConfig indexerConfig,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      BlobStore blobStore,
      AstraConfigs.S3Config s3Config) {

    ChunkRollOverStrategy chunkRollOverStrategy =
        DiskOrMessageCountBasedRolloverStrategy.fromConfig(meterRegistry, indexerConfig);

    return new IndexingChunkManager<>(
        CHUNK_DATA_PREFIX,
        indexerConfig.getDataDirectory(),
        chunkRollOverStrategy,
        meterRegistry,
        blobStore,
        makeDefaultRollOverExecutor(),
        curatorFramework,
        SearchContext.fromConfig(indexerConfig.getServerConfig()),
        indexerConfig,
        metadataStoreConfig);
  }
}
