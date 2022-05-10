package com.slack.kaldb.chunkManager;

public class NeverRolloverChunkStrategyImpl implements ChunkRollOverStrategy {
  @Override
  public boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed) {
    return false;
  }
}
