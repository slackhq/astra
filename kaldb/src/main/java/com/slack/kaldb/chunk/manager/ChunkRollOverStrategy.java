package com.slack.kaldb.chunk.manager;

// TODO: Rename ChunkRollOverPolicyPredicate
public interface ChunkRollOverStrategy {
  boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed);
}
