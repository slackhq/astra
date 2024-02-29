package com.slack.kaldb.chunk;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.slack.kaldb.util.ArgValidationUtils.ensureNonNullString;

import com.slack.kaldb.logstore.LogStore;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;

/**
 * The RecoveryChunkFactoryImpl is a chunk factory that contains the context needed to create a
 * recovery chunk.
 *
 * @param <T> Type of messages stored in chunk.
 */
public class RecoveryChunkFactoryImpl<T> implements ChunkFactory<T> {

  private final String chunkDataPrefix;
  private final MeterRegistry meterRegistry;
  private final SearchMetadataStore searchMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final SearchContext searchContext;
  private final KaldbConfigs.IndexerConfig indexerConfig;
  private String kafkaPartitionId = null;

  public RecoveryChunkFactoryImpl(
      KaldbConfigs.IndexerConfig indexerConfig,
      String chunkDataPrefix,
      MeterRegistry meterRegistry,
      SearchMetadataStore searchMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      SearchContext searchContext) {
    checkNotNull(indexerConfig, "indexerConfig can't be null");
    this.indexerConfig = indexerConfig;
    this.chunkDataPrefix = chunkDataPrefix;
    this.meterRegistry = meterRegistry;
    this.searchMetadataStore = searchMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.searchContext = searchContext;
  }

  @Override
  public ReadWriteChunk<T> makeChunk() throws IOException {
    ensureNonNullString(kafkaPartitionId, "kafkaPartitionId can't be null and should be set.");
    ensureNonNullString(indexerConfig.getDataDirectory(), "The data directory shouldn't be empty");
    final File dataDirectory = new File(indexerConfig.getDataDirectory());
    LogStore logStore =
        LuceneIndexStoreImpl.makeLogStore(
            dataDirectory, indexerConfig.getLuceneConfig(), meterRegistry);

    return new RecoveryChunkImpl<>(
        logStore,
        chunkDataPrefix,
        meterRegistry,
        searchMetadataStore,
        snapshotMetadataStore,
        searchContext,
        kafkaPartitionId);
  }

  @Override
  public void setKafkaPartitionId(String kafkaPartitionId) {
    this.kafkaPartitionId = kafkaPartitionId;
  }
}
