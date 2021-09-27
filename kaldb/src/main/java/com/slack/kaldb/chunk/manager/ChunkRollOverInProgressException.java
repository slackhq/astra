package com.slack.kaldb.chunk.manager;

public class ChunkRollOverInProgressException extends RuntimeException {
  public ChunkRollOverInProgressException(String message) {
    super(message);
  }
}
