package com.slack.kaldb.chunk.manager;

public class ChunkRollOverException extends RuntimeException {
  public ChunkRollOverException(String message) {
    super(message);
  }
}
