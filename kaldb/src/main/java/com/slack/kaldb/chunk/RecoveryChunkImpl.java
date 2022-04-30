package com.slack.kaldb.chunk;

import static com.slack.kaldb.chunk.ChunkInfo.toSnapshotMetadata;

import com.slack.kaldb.logstore.LogStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A recovery chunk is a read write chunk used in the recovery service. This chunk can be written to
 * but for now we don't allow any reads on the recovery chunk. However, instead of blocking the read
 * API, it achieves this outcome by not registering a search metadata node or a live snapshot node.
 * So, query nodes can't find the recovery node.
 *
 * <p>A recovery chunk is optimized for indexing the data as fast as it can. So, we don't plan to
 * expose it for reads for the time being.
 */
public class RecoveryChunkImpl<T> extends ReadWriteChunk<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyChunkImpl.class);

  public RecoveryChunkImpl(
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
  public void postCreate() {
    // no action since we are not registering a live node or a search index.
  }

  @Override
  public void postSnapshot() {
    LOG.info("Start post snapshot for recovery chunk {}", chunkInfo);
    // Publish a persistent snapshot for this chunk.
    SnapshotMetadata nonLiveSnapshotMetadata = toSnapshotMetadata(chunkInfo, "");
    snapshotMetadataStore.createSync(nonLiveSnapshotMetadata);
    LOG.info("Post snapshot operation completed for recovery chunk {}", chunkInfo);
  }

  @Override
  public void preClose() {
    // Nothing to delete since we created no live nodes.
  }
}
