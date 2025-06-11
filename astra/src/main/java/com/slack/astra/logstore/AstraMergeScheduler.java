package com.slack.astra.logstore;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.MergePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AstraMergeScheduler extends ConcurrentMergeScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(AstraMergeScheduler.class);
  private MeterRegistry metricsRegistry;

  public static final String STALL_TIME = "astra_index_merge_stall_time_ms";
  private final Counter stallCounter;

  public final String STALL_THREADS = "astra_index_merge_stall_threads";
  private AtomicInteger activeStallThreadsCount;

  public final String MERGE_COUNTER = "astra_index_merge_count";
  private final Counter mergeCounter;

  public AstraMergeScheduler(MeterRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
    stallCounter = this.metricsRegistry.counter(STALL_TIME);
    activeStallThreadsCount = this.metricsRegistry.gauge(STALL_THREADS, new AtomicInteger());
    this.mergeCounter = this.metricsRegistry.counter(MERGE_COUNTER);
  }

  @Override
  protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
    // We can use `merge` to get more stats when we want to tune further
    LOG.debug("Starting merge");
    mergeCounter.increment();
    super.doMerge(mergeSource, merge);
    LOG.debug("Ending merge");
  }

  /**
   * ConcurrentMergeScheduler#maybeStall provides a good description of what this method does. Here
   * I want to explain our motivation for adding metrics around this - This method is called when we
   * call IndexWriter#commit ( which calls ConcurrentMergeScheduler#merge which calls this method )
   * We want to capture how many indexing threads are blocked because merges are falling behind We
   * also want to capture total stalled time The motivation being we could optimize indexing ( say
   * offline indexing ) based on this knowledge https://issues.apache.org/jira/browse/LUCENE-6119
   * has details on why Lucene added auto IO throttle
   */
  @Override
  protected synchronized boolean maybeStall(MergeSource mergeSource) {
    long startTime = System.nanoTime();
    activeStallThreadsCount.incrementAndGet();

    boolean paused = super.maybeStall(mergeSource);

    long elapsed = System.nanoTime() - startTime;
    stallCounter.increment((double) elapsed);
    activeStallThreadsCount.decrementAndGet();

    return paused;
  }
}
