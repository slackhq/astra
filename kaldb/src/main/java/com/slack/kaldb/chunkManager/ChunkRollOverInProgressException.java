package com.slack.kaldb.chunkManager;

public class ChunkRollOverInProgressException extends RuntimeException {
  public ChunkRollOverInProgressException(String message) {
    super(message);
  }
}
