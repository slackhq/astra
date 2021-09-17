package com.slack.kaldb.chunk.manager.indexing;

public class ChunkRollOverInProgressException extends RuntimeException {
  public ChunkRollOverInProgressException(String message) {
    super(message);
  }
}
