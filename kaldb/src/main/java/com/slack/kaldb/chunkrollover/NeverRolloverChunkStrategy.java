package com.slack.kaldb.chunkrollover;

import org.apache.lucene.store.FSDirectory;

/**
 * The NeverRolloverChunkStrategy always responds in the negative for a chunk roll over request. It
 * is currently used in recovery service which doesn't roll over the chunk.
 */
public class NeverRolloverChunkStrategy implements ChunkRollOverStrategy {
  @Override
  public boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed) {
    return false;
  }

  @Override
  public void setActiveChunkDirectory(FSDirectory activeChunkDirectory) {}

  @Override
  public void close() {}
}
