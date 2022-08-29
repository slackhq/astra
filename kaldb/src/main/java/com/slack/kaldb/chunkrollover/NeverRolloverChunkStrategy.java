package com.slack.kaldb.chunkrollover;

import java.io.File;

/**
 * The NeverRolloverChunkStrategy always responds in the negative for a chunk roll over request.
 * It is currently used in recovery service which doesn't roll over the chunk.
 */
public class NeverRolloverChunkStrategy implements ChunkRollOverStrategy {
  @Override
  public boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed) {
    return false;
  }

  @Override
  public void setActiveChunkDirectory(File activeChunkDirectory) {}

  @Override
  public void close() {}
}
