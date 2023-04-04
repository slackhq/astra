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
 * A recovery chunk is a read write chunk used in the recovery service. A recovery chunk is
 * optimized for indexing the data as fast as it can. So, we don't plan to expose it for reads for
 * the time being.
 *
 * <p>To prevent external queries, a recovery node doesn't publish any live snapshots or search
 * nodes to be queried by query nodes. We don't block the local read API since we need it for tests
 * and helps with code reuse.
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
      String kafkaPartitionId,
      boolean enableFullTextSearch) {
    super(
        logStore,
        chunkDataPrefix,
        meterRegistry,
        searchMetadataStore,
        snapshotMetadataStore,
        searchContext,
        kafkaPartitionId,
        LOG,
        enableFullTextSearch);
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
