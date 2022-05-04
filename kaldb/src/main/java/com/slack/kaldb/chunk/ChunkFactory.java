package com.slack.kaldb.chunk;

import java.io.IOException;

public interface ChunkFactory<T> {
  ReadWriteChunk<T> makeChunk() throws IOException;

  void setKafkaPartitionId(String kafkaPartitionId);
}
