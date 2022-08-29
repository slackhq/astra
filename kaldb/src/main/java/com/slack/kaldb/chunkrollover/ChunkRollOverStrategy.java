package com.slack.kaldb.chunkrollover;

import java.io.File;

// TODO: Rename ChunkRollOverPolicyPredicate
public interface ChunkRollOverStrategy {
  boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed);

  public void setActiveChunkDirectory(File activeChunkDirectory);

  public void close();
}
