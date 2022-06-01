package com.slack.kaldb.chunk;

import java.io.IOException;

/**
 * A chunk factory makes a chunk. It is a container class that carries all the context needed to
 * create a chunk. For context that is dynamic, it provides apis to set it like kafkaPartitionId.
 *
 * @param <T> Type of messages stored in chunk.
 */
public interface ChunkFactory<T> {
  ReadWriteChunk<T> makeChunk() throws IOException;

  void setKafkaPartitionId(String kafkaPartitionId);
}
