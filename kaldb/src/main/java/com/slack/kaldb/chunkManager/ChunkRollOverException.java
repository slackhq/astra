package com.slack.kaldb.chunkManager;

public class ChunkRollOverException extends RuntimeException {
  public ChunkRollOverException(String message) {
    super(message);
  }
}
