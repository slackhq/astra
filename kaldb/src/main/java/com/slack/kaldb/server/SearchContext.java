package com.slack.kaldb.server;

import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.proto.config.KaldbConfigs;

public class SearchContext {
  public static SearchContext fromConfig(KaldbConfigs.ServerConfig serverConfig) {
    return new SearchContext(serverConfig.getServerAddress(), serverConfig.getServerPort());
  }

  public final String hostname;
  public final int port;

  public SearchContext(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  public SearchMetadata toSearchMetadata(String snapshotName) {
    return new SearchMetadata(hostname, snapshotName, toUrl());
  }

  public String toUrl() {
    return hostname + ":" + port;
  }
}
