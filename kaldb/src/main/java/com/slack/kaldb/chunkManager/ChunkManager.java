package com.slack.kaldb.chunkManager;

import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import java.io.IOException;

public interface ChunkManager<T> {
  void addMessage(T message, long msgSize, String kafkaPartitionId, long offset) throws IOException;

  SearchResult<T> query(SearchQuery query, long timeoutMs);
}
