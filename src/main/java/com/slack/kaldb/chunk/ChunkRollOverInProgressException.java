package com.slack.kaldb.chunk;

public class ChunkRollOverInProgressException extends RuntimeException {
  public ChunkRollOverInProgressException(String message) {
    super(message);
  }
}
