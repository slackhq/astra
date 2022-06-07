package com.slack.kaldb.chunk;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RateLimitedChunkDownloaderImpl is a decorator around a chunk downloader, that limits the number
 * chunk downloaders that can execute in parallel.
 */
public class RateLimitedChunkDownloaderImpl implements ChunkDownloader {
  private static final Logger LOG = LoggerFactory.getLogger(RateLimitedChunkDownloaderImpl.class);

  private final ExecutorService executor;
  private final ChunkDownloader chunkDownloader;

  public RateLimitedChunkDownloaderImpl(
      ChunkDownloader chunkDownloader, ExecutorService executorService) {
    this.chunkDownloader = chunkDownloader;
    this.executor = executorService;
  }

  @Override
  public boolean download() throws Exception {
    LOG.info("Starting rate limited chunk download.");
    Future<Boolean> future = executor.submit(chunkDownloader::download);
    Boolean result = future.get();
    LOG.info("Finished rate limited chunk download.");
    return result;
  }
}
