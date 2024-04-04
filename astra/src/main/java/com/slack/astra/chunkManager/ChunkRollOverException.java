package com.slack.astra.chunkManager;

public class ChunkRollOverException extends RuntimeException {
  public ChunkRollOverException(String message) {
    super(message);
  }
}
