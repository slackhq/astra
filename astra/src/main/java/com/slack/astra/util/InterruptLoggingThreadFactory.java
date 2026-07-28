package com.slack.astra.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// In short: makes threads that log "who interrupted me, and from where" so we can find the code
// that is breaking the indexer's write lock. It only watches; it does not change any behavior.

/**
 * A {@link ThreadFactory} whose threads log the caller's stack trace whenever {@link
 * Thread#interrupt()} is invoked on them, before delegating to {@link Thread#interrupt()}.
 *
 * <p>This exists purely to diagnose the production "FileLock invalidated by an external force"
 * crashes. A leaked interrupt flag on a reused Lucene-I/O pool thread (scheduled commit/refresh,
 * rollover) is eaten by the next channel op and permanently invalidates the shared write.lock for
 * the whole chunk. Lucene will not defend against this (see LUCENE-8262 and apache/lucene#9309,
 * both resolved "Won't Fix"); their guidance is that callers must not interrupt threads performing
 * Lucene I/O. We do not yet know what interrupts these threads in steady state.
 *
 * <p>Because nothing outside those classes holds a reference to these threads, the interrupt must
 * originate either from an executor shutdown escalation ({@code shutdownNow()}) or from Lucene
 * internals restoring the flag ({@code Thread.currentThread().interrupt()}) after catching an
 * {@link InterruptedException}. Overriding {@code interrupt()} captures the caller stack in either
 * case, which is the piece the JVM otherwise gives us no way to observe.
 *
 * <p>This factory only logs; it does not clear or suppress the interrupt. It is a diagnostic, not a
 * fix.
 */
public class InterruptLoggingThreadFactory implements ThreadFactory {

  private static final Logger LOG = LoggerFactory.getLogger(InterruptLoggingThreadFactory.class);

  private final ThreadFactory delegate;

  public InterruptLoggingThreadFactory(String nameFormat) {
    // Guava's ThreadFactoryBuilder owns the "%d" naming/counter convention used across the
    // codebase; we only add the interrupt() override on top of the threads it produces.
    this.delegate =
        new ThreadFactoryBuilder()
            .setNameFormat(nameFormat)
            .setThreadFactory(
                r ->
                    new Thread(r) {
                      @Override
                      public void interrupt() {
                        // Capture who is interrupting a Lucene-I/O thread. The Throwable is never
                        // thrown -- it only records the calling stack synchronously at interrupt.
                        LOG.warn(
                            "Thread '{}' performing Lucene I/O was interrupted; capturing caller "
                                + "stack to identify the write.lock-invalidation trigger",
                            getName(),
                            new Throwable("interrupt() caller stack"));
                        super.interrupt();
                      }
                    })
            .build();
  }

  @Override
  public Thread newThread(Runnable r) {
    return delegate.newThread(r);
  }
}
