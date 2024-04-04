package com.slack.astra.chunk;

/** A ChunkDownloader is used to download chunk data from a remote store a local store. */
public interface ChunkDownloader {
  boolean download() throws Exception;
}
