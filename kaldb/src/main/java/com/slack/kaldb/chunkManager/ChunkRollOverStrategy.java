package com.slack.kaldb.chunkManager;

import java.io.File;

// TODO: Rename ChunkRollOverPolicyPredicate
public interface ChunkRollOverStrategy {
  boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed);

  public void setActiveChunkDirectory(File activeChunkDirectory);

  public long getApproximateDirectoryBytes();

  public void close();
}
