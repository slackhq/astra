package com.slack.kaldb.chunk.manager.indexing;

public class ChunkRollOverException extends RuntimeException {
  public ChunkRollOverException(String message) {
    super(message);
  }
}
