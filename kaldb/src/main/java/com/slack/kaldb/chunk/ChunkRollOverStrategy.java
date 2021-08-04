package com.slack.kaldb.chunk;

// TODO: Rename ChunkRollOverPolicyPredicate
public interface ChunkRollOverStrategy {
  boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed);
}
