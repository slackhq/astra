package com.slack.kaldb.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.*;

public class MetricCollectingThreadPoolExecutor extends ThreadPoolExecutor {

  public String threadNameFormat;
  public MeterRegistry meterRegistry;

  private final String CORE_POOL_SIZE = "_core_pool_size";
  private final String ACTIVE_THREADS = "_active_threads";
  private final String MAX_POOL_SIZE = "_max_pool_size";
  private final String QUEUE_SIZE = "_queue_size";

  // Mimics Executors.newFixedThreadPool but without the static so that we can initialize metrics
  public MetricCollectingThreadPoolExecutor(int nThreads, String threadNameFormat, MeterRegistry meterRegistry) {
    this(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
    this.threadNameFormat = threadNameFormat;
    this.meterRegistry = meterRegistry;
    registerGauges();
  }

  public MetricCollectingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
  }

  private void registerGauges() {
    meterRegistry.gauge(threadNameFormat + CORE_POOL_SIZE, getCorePoolSize());
    meterRegistry.gauge(threadNameFormat + ACTIVE_THREADS, getActiveCount());
    meterRegistry.gauge(threadNameFormat + MAX_POOL_SIZE, getMaximumPoolSize());
    meterRegistry.gauge(threadNameFormat + QUEUE_SIZE, getQueue().size());
  }

}
