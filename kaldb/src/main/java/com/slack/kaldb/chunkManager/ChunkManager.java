package com.slack.kaldb.chunkManager;

import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;

public interface ChunkManager<T> {
  void addMessage(Trace.Span message, long msgSize, String kafkaPartitionId, long offset)
      throws IOException;

  SearchResult<T> query(SearchQuery query, Duration queryTimeout);

  Map<String, FieldType> getSchema();
}
