package com.slack.kaldb.chunk;

public interface SnapshotDownloader {
  boolean download() throws Exception;
}
