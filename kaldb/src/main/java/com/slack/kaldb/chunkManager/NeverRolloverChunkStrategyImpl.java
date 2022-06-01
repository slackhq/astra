package com.slack.kaldb.chunkManager;

/**
 * The NeverRolloverChunkStrategyImpl always responds in the negative for a chunk roll over request.
 * It is currently used in recovery service which doesn't roll over the chunk.
 */
public class NeverRolloverChunkStrategyImpl implements ChunkRollOverStrategy {
  @Override
  public boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed) {
    return false;
  }
}
