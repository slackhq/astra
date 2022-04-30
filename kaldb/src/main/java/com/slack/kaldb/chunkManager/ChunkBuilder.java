package com.slack.kaldb.chunkManager;

import com.slack.kaldb.chunk.ReadWriteChunk;
import java.io.IOException;

public interface ChunkBuilder<T> {
  ReadWriteChunk<T> build() throws IOException;

  void setKafkaPartitionId(String kafkaPartitionId);
}
