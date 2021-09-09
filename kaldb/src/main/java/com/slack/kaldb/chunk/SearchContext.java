package com.slack.kaldb.chunk;

import static com.google.common.base.Preconditions.checkState;

import com.slack.kaldb.proto.config.KaldbConfigs;

public class SearchContext {
  public static SearchContext fromConfig(KaldbConfigs.ServerConfig serverConfig) {
    return new SearchContext(serverConfig.getServerAddress(), serverConfig.getServerPort());
  }

  public final String hostname;
  public final int port;

  public SearchContext(String hostname, int port) {
    checkState(hostname != null && !hostname.isEmpty(), "hostname field can't be null or empty");
    checkState(port > 0, "port value has to be a positive number.");

    this.hostname = hostname;
    this.port = port;
  }

  public String toUrl() {
    return hostname + ":" + port;
  }
}
