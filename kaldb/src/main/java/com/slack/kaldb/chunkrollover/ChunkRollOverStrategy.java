package com.slack.kaldb.chunkrollover;

import java.io.File;

// TODO: ChunkRollOverStrategy should take a chunk as an input and get statistics
//  like message count, size etc. from the chunk
public interface ChunkRollOverStrategy {
  boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed);

  public void setActiveChunkDirectory(File activeChunkDirectory);

  public void close();
}
