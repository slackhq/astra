package com.slack.kaldb.chunk;

import static com.slack.kaldb.chunk.ChunkInfo.toSnapshotMetadata;

import com.slack.kaldb.logstore.LogStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingReadWriteChunkImpl<T> extends ReadWriteChunk<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadWriteChunk.class);

  public IndexingReadWriteChunkImpl(
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
        kafkaPartitionId);
  }

  @Override
  public void postSnapshot() {
    LOG.info("Start Post snapshot chunk {}", chunkInfo);
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
            chunkInfo.getKafkaPartitionId());
    snapshotMetadataStore.updateSync(updatedSnapshotMetadata);
    liveSnapshotMetadata = updatedSnapshotMetadata;

    LOG.info("Post snapshot operation completed for RW chunk {}", chunkInfo);
  }

  public void register() {
    snapshotMetadataStore.createSync(liveSnapshotMetadata);
    searchMetadataStore.createSync(liveSearchMetadata);
  }

  public void deRegister() {
    searchMetadataStore.deleteSync(liveSearchMetadata);
    snapshotMetadataStore.deleteSync(liveSnapshotMetadata);
  }
}
