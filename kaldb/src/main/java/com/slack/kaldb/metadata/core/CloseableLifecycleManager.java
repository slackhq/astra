package com.slack.kaldb.metadata.core;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each instance of CloseableLifecycleManager holds a list of closables in a managed guava service.
 * The caller needs to add the created instance to a service manager which would then take care of
 * the object lifecycle.
 */
public class CloseableLifecycleManager extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(CloseableLifecycleManager.class);

  private final KaldbConfigs.NodeRole nodeRole;
  private final List<Closeable> closeableList;

  public CloseableLifecycleManager(KaldbConfigs.NodeRole role, List<Closeable> closeableList) {
    this.closeableList = closeableList;
    this.nodeRole = role;
  }

  @Override
  protected void startUp() throws Exception {}

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down CloseableLifecycleManager for role - {}", nodeRole.toString());
    closeableList.forEach(
        closeable -> {
          try {
            closeable.close();
          } catch (IOException e) {
            LOG.error(
                "Exception shutting down closeable - {}, ignoring and continuing shutdown",
                closeable.getClass());
          }
        });
  }
}
