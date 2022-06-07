package com.slack.kaldb.chunk;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * RateLimitedChunkDownloaderImpl is a decorator around a chunk downloader, that limits the number
 * chunk downloaders that can execute in parallel.
 */
public class RateLimitedChunkDownloaderImpl implements ChunkDownloader {
  private final ExecutorService executor;
  private final ChunkDownloader chunkDownloader;

  public RateLimitedChunkDownloaderImpl(
      ChunkDownloader chunkDownloader, ExecutorService executorService) {
    this.chunkDownloader = chunkDownloader;
    this.executor = executorService;
  }

  @Override
  public boolean download() throws Exception {
    Future<Boolean> future = executor.submit(chunkDownloader::download);
    return future.get();
  }
}
