package com.slack.kaldb.chunkManager;

import com.slack.kaldb.chunk.Chunk;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.metadata.schema.FieldType;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public interface ChunkManager<T> {
  void addMessage(T message, long msgSize, String kafkaPartitionId, long offset) throws IOException;

  SearchResult<T> query(SearchQuery query, Duration queryTimeout);

  Map<String, FieldType> getSchema();

  List<Chunk<T>> getChunkList();

  void removeStaleChunks(List<Chunk<T>> staleChunks);
}
