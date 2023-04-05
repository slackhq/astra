package com.slack.kaldb.chunk;

import static com.slack.kaldb.chunk.ChunkInfo.toSnapshotMetadata;

import com.slack.kaldb.logstore.LogStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IndexingChunkImpl provides a concrete implementation for a chunk used in the indexer process.
 * One can write and read the messages we wrote to this chunk. It provides a unified interface of a
 * shard abstracting the details of the underlying storage implementation.
 *
 * <p>This class relies on ReadWriteChunk base class for a majority of it's functionality.
 *
 * <p>Chunk maintains its metadata in the chunkInfo object. The chunkInfo tracks all the info needed
 * for constructing a snapshot.
 *
 * <p>A indexing chunk goes through the following life cycle. When a chunk is created it is open for
 * both reads and writes. Since a indexing chunk is ingesting live data, a cluster manager doesn't
 * manage it. Instead, when a chunk in created, it registers a live snapshot and a live search node
 * in the postCreation method.
 *
 * <p>Once the chunk is full, it will be snapshotted. Once snapshotted, the chunk is not open for
 * writing anymore. When a chunk is snapshotted, a non-live snapshot is created which is assigned to
 * a cache node by the cluster manager. The live snapshot is updated with the end time of the chunk
 * so it only receives the queries for the data within it's time range. As long as the chunk is up,
 * it will be searched using the live search node. This logic is implemented in the overloaded
 * postSnapshot method in this class.
 *
 * <p>When the ReadWriteChunk is finally closed (happens when a chunk is evicted), the live snapshot
 * and the search metadata are deleted as part of the chunk de-registration process in the preClose
 * method.
 */
public class IndexingChunkImpl<T> extends ReadWriteChunk<T> {
  private static final Logger LOG = LoggerFactory.getLogger(IndexingChunkImpl.class);

  public IndexingChunkImpl(
      LogStore<T> logStore,
      String chunkDataPrefix,
      MeterRegistry meterRegistry,
      SearchMetadataStore searchMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      SearchContext searchContext,
      String kafkaPartitionId) {
    super(
        logStore,
        chunkDataPrefix,
        meterRegistry,
        searchMetadataStore,
        snapshotMetadataStore,
        searchContext,
        kafkaPartitionId,
        LOG);
  }

  @Override
  public void postSnapshot() {
    LOG.info("Start post snapshot chunk {}", chunkInfo);
    // Publish a persistent snapshot for this chunk.
    SnapshotMetadata nonLiveSnapshotMetadata = toSnapshotMetadata(chunkInfo, "");
    snapshotMetadataStore.createSync(nonLiveSnapshotMetadata);

    // Update the live snapshot. Keep the same snapshotId and snapshotPath to
    // ensure it's a live snapshot.
    SnapshotMetadata updatedSnapshotMetadata =
        new SnapshotMetadata(
            liveSnapshotMetadata.snapshotId,
            liveSnapshotMetadata.snapshotPath,
            chunkInfo.getDataStartTimeEpochMs(),
            chunkInfo.getDataEndTimeEpochMs(),
            chunkInfo.getMaxOffset(),
            chunkInfo.getKafkaPartitionId(),
            Metadata.IndexType.LOGS_LUCENE9);
    snapshotMetadataStore.updateSync(updatedSnapshotMetadata);
    liveSnapshotMetadata = updatedSnapshotMetadata;

    LOG.info("Post snapshot operation completed for indexing RW chunk {}", chunkInfo);
  }

  @Override
  public void postCreate() {
    snapshotMetadataStore.createSync(liveSnapshotMetadata);
    searchMetadataStore.createSync(liveSearchMetadata);
  }

  @Override
  public void preClose() {
    searchMetadataStore.deleteSync(liveSearchMetadata);
    snapshotMetadataStore.deleteSync(liveSnapshotMetadata);
  }
}
