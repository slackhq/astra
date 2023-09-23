package com.slack.kaldb.server;

import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

// https://github.com/line/armeria/issues/4911
public class ArmeriaVirtualBlockingTaskExecutor {

  private static final ThreadFactory virtualThreadFactory =
      Thread.ofVirtual().name("armeria-blocking-tasks-virtual-threads-", 0).factory();

  private static final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
      new ScheduledThreadPoolExecutor(200, virtualThreadFactory);

  private static final BlockingTaskExecutor blockingTaskExecutor =
      BlockingTaskExecutor.of(scheduledThreadPoolExecutor);

  public static BlockingTaskExecutor virtualBlockingTaskExecutor() {
    return blockingTaskExecutor;
  }

  private ArmeriaVirtualBlockingTaskExecutor() {}
}
