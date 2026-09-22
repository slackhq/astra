package com.slack.astra.metadata.core;

import com.slack.astra.metadata.core.EtcdErrorClassifier.Recovery;
import com.slack.astra.proto.config.AstraConfigs.EtcdConfig;
import com.slack.astra.util.ExponentialBackOff;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Re-establishes one etcd watch stream after failure: callers supply how to open a watch at a given
 * revision, this class decides when and from where.
 */
final class EtcdWatchRetry {

  private static final Logger LOG = LoggerFactory.getLogger(EtcdWatchRetry.class);

  /** Open the watch at whatever revision etcd is currently at. */
  static final long REVISION_LATEST = 0;

  /** Re-list the store, adopt the server's revision, and watch from there. */
  static final long REVISION_RESYNC = -1;

  /** Consecutive failures after which a resync is forced however the error classified. */
  private static final int FORCE_RESYNC_AFTER_FAILURES = 3;

  /** {@link #establishedAtMs} when no stream is currently open. */
  private static final long NOT_ESTABLISHED = -1;

  private static final String ASTRA_ETCD_WATCH_RETRY = "astra_etcd_watch_retry";
  private static final String ASTRA_ETCD_WATCH_RETRY_DELAY = "astra_etcd_watch_retry_delay";

  private final ScheduledExecutorService executor;
  private final BooleanSupplier suppressed;
  private final MeterRegistry meterRegistry;
  private final String storeTag;

  /** Opens a watch at the given revision, handed {@code this} to report its own failures back. */
  private final BiConsumer<Long, EtcdWatchRetry> reopen;

  /** Continuous failing time after which the watch is unrecoverable and the JVM halts. */
  private final long fatalAfterMs;

  /** What is retrying, for logs: e.g. {@code "watch for store /snapshot"}. */
  private final String subject;

  /** What being down costs, for logs: e.g. {@code "cache is stale"}. */
  private final String staleConsequence;

  /** Paces attempts with ±50% jitter and an unbounded budget, so it never returns STOP. */
  private final ExponentialBackOff backoff;

  /** Revision the next attempt resumes from; written by jetcd callback threads. */
  private final AtomicLong resumeRevision = new AtomicLong(REVISION_RESYNC);

  /** How long an open stream must survive to count as recovered rather than flapping. */
  private final long stableAfterMs;

  /** Injected so tests can drive {@link #streamWasStable()} without sleeping. */
  private final LongSupplier clock;

  /** When the currently open stream was opened, or {@link #NOT_ESTABLISHED} if none is. */
  private long establishedAtMs = NOT_ESTABLISHED;

  private int consecutiveFailures;

  EtcdWatchRetry(
      EtcdConfig config,
      MeterRegistry meterRegistry,
      String storeTag,
      ScheduledExecutorService executor,
      BooleanSupplier suppressed,
      BiConsumer<Long, EtcdWatchRetry> reopen,
      String subject,
      String staleConsequence,
      LongSupplier clock) {
    this.meterRegistry = meterRegistry;
    this.storeTag = storeTag;
    this.executor = executor;
    this.suppressed = suppressed;
    this.reopen = reopen;
    this.subject = subject;
    this.staleConsequence = staleConsequence;
    this.clock = clock;
    this.fatalAfterMs =
        EtcdMetadataStore.positiveOrDefault(
            config.getWatchFatalAfterMs(), EtcdMetadataStore.DEFAULT_WATCH_FATAL_AFTER_MS);
    long initialIntervalMs =
        EtcdMetadataStore.positiveOrDefault(
            config.getInitialRetryIntervalMs(),
            EtcdMetadataStore.DEFAULT_INITIAL_RETRY_INTERVAL_MS);
    // The backoff ceiling doubles as the stability threshold.
    this.stableAfterMs =
        EtcdMetadataStore.positiveOrDefault(
            config.getMaxRetryDelayMs(), EtcdMetadataStore.DEFAULT_MAX_RETRY_DELAY_MS);
    this.backoff = new ExponentialBackOff(initialIntervalMs, stableAfterMs, Long.MAX_VALUE, clock);
  }

  /**
   * Records a revision a response was delivered at, monotonically so a queued stale response cannot
   * drag the resume point back.
   */
  void reachedRevision(long revision) {
    resumeRevision.accumulateAndGet(revision, Math::max);
  }

  /**
   * Adopts the revision a watch is being opened at, moving backwards if told to since a cluster
   * restored from a snapshot rewinds.
   */
  void openedAtRevision(long revision) {
    resumeRevision.set(revision);
  }

  /** Forces the next attempt to re-list, for when no usable revision was ever learned. */
  void requireResync() {
    resumeRevision.set(REVISION_RESYNC);
  }

  /**
   * Whether a watch callback should drive recovery: not during close, and only the first callback
   * per stream, since an error and a completion both arrive for the same dead stream.
   */
  boolean claimDisposal(AtomicBoolean disposed, String event) {
    if (suppressed.getAsBoolean()) {
      LOG.debug("Ignoring {} during close of {}", event, subject);
      return false;
    }
    if (!disposed.compareAndSet(false, true)) {
      LOG.debug("Ignoring {} for already-disposed stream of {}", event, subject);
      return false;
    }
    return true;
  }

  /**
   * Records that a stream is open again, without clearing pacing: a leader election accepts the
   * watch and cancels it moments later, so this runs on every attempt of a failing episode.
   */
  synchronized void onEstablished() {
    establishedAtMs = clock.getAsLong();
    if (consecutiveFailures == 0) {
      LOG.info("Successfully established initial {}", subject);
    } else if (consecutiveFailures == 1) {
      LOG.info("Successfully re-established {} after 1 attempt", subject);
    } else {
      // onFailure already reported every loss at WARN; repeating the open at INFO doubles volume.
      LOG.debug("Re-opened {} after {} consecutive failures", subject, consecutiveFailures);
    }
  }

  /**
   * Schedules the next attempt after a backoff delay, or halts past {@link #fatalAfterMs}.
   *
   * @param context what failed, for the log line
   */
  synchronized void onFailure(Throwable error, String context) {
    Recovery recovery = EtcdErrorClassifier.classify(error);

    if (streamWasStable()) {
      // The stream recovered, so this failure opens a new episode.
      backoff.reset();
      consecutiveFailures = 0;
    }
    establishedAtMs = NOT_ESTABLISHED;

    consecutiveFailures++;
    // Counted for the fatal failure too, so the last classification explains the halt.
    meterRegistry
        .counter(ASTRA_ETCD_WATCH_RETRY, "store", storeTag, "reason", recovery.reason())
        .increment();

    if (recovery.isTerminal()) {
      LOG.warn(
          "{} — the etcd client behind {} is closed, so no reconnect can succeed; leaving the watch"
              + " down ({}): {}",
          context,
          subject,
          staleConsequence,
          String.valueOf(error));
      return;
    }

    long retryRevision = resumeRevisionFor(recovery);
    long delayMs = backoff.nextBackOffMillis();

    long elapsedMs = backoff.getElapsedTimeMs();
    if (elapsedMs >= fatalAfterMs) {
      LOG.error(
          "{} down for {} ms over {} attempts ({}); this is far past any leader election or"
              + " partition, treating the watch as unrecoverable",
          subject,
          elapsedMs,
          consecutiveFailures,
          recovery.reason());
      EtcdMetadataStore.handleFatalAsync(error, "watch-" + subject);
      return;
    }
    LOG.atWarn()
        // Stack trace once per episode; retries never stop, and every watch logs its own.
        .setCause(consecutiveFailures == 1 ? error : null)
        .log(
            "{} ({}) — down {} ms over {} attempts, {}, retrying in {} ms: {}",
            context,
            recovery.reason(),
            elapsedMs,
            consecutiveFailures,
            staleConsequence,
            delayMs,
            String.valueOf(error));

    meterRegistry
        .timer(ASTRA_ETCD_WATCH_RETRY_DELAY, "store", storeTag)
        .record(delayMs, TimeUnit.MILLISECONDS);
    try {
      executor.schedule(() -> reopen.accept(retryRevision, this), delayMs, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      LOG.info("Abandoning retry of {}, store is closing", subject);
    }
  }

  /** Whether the stream that just failed had been up long enough to count as recovered. */
  private boolean streamWasStable() {
    return establishedAtMs != NOT_ESTABLISHED
        && clock.getAsLong() - establishedAtMs >= stableAfterMs;
  }

  /** Chooses the revision the next attempt opens at. */
  private long resumeRevisionFor(Recovery recovery) {
    long preferred = resumeRevision.get();
    if (recovery.requiresResync() || preferred == REVISION_RESYNC) {
      return REVISION_RESYNC;
    }
    if (consecutiveFailures == FORCE_RESYNC_AFTER_FAILURES) {
      LOG.warn(
          "Forcing resync of {} after {} consecutive failures at revision {}",
          subject,
          consecutiveFailures,
          preferred);
      return REVISION_RESYNC;
    }
    return preferred;
  }
}
