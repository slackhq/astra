package com.slack.kaldb.chunk;

public class ChunkRollOverException extends RuntimeException {
  public ChunkRollOverException(String message) {
    super(message);
  }
}
