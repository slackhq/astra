package com.slack.astra.chunkrollover;

import org.apache.lucene.store.FSDirectory;

// TODO: ChunkRollOverStrategy should take a chunk as an input and get statistics
//  like message count, size etc. from the chunk
public interface ChunkRollOverStrategy {
  boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed);

  public void setActiveChunkDirectory(FSDirectory directory);

  public void close();
}
