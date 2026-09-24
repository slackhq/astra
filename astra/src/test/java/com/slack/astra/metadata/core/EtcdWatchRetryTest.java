package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.common.exception.ErrorCode;
import io.etcd.jetcd.common.exception.EtcdExceptionFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies which revision a watch attempt resumes from and how attempts are paced, driven through a
 * fake clock since every threshold in the policy is tens of seconds to minutes.
 */
public class EtcdWatchRetryTest {

  private static final long INITIAL_MS = 100;
  private static final long MAX_MS = 1_000;
  private static final long FATAL_MS = 60_000;

  private static final AstraConfigs.EtcdConfig CONFIG =
      AstraConfigs.EtcdConfig.newBuilder()
          .setInitialRetryIntervalMs((int) INITIAL_MS)
          .setMaxRetryDelayMs((int) MAX_MS)
          .setWatchFatalAfterMs((int) FATAL_MS)
          .build();

  /** Attempts needed for the backoff to walk 100ms to its 1000ms ceiling at a 1.5x multiplier. */
  private static final int ATTEMPTS_TO_CEILING = 7;

  /** The backoff jitters ±50%, so only half of a nominal interval is guaranteed. */
  private static final double JITTER_FLOOR = 0.5;

  private static final Throwable TRANSIENT =
      EtcdExceptionFactory.newEtcdException(ErrorCode.UNAVAILABLE, "etcdserver: leader changed");
  private static final Throwable FUTURE_REVISION =
      EtcdExceptionFactory.newEtcdException(
          ErrorCode.OUT_OF_RANGE, "etcdserver: mvcc: required revision is a future revision");
  private static final Throwable COMPACTED = EtcdExceptionFactory.newCompactedException(42);
  private static final Throwable UNCLASSIFIED_REVISION =
      EtcdExceptionFactory.newEtcdException(ErrorCode.OUT_OF_RANGE, null);
  private static final Throwable CLIENT_CLOSED =
      EtcdExceptionFactory.newClosedWatchClientException();

  /** A revision the stream is pretending to have reached before it failed. */
  private static final long REACHED = 500;

  /** A revision below {@link #REACHED}, as a cluster restored from a snapshot would report. */
  private static final long REWOUND = 100;

  private static final String STORE_TAG = "/test";

  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
  private final AtomicLong nowMs = new AtomicLong(1_000);
  private final RecordingExecutor executor = new RecordingExecutor();
  private final List<Long> reopenedAt = new ArrayList<>();

  private EtcdWatchRetry retry;

  @BeforeEach
  public void setUp() {
    retry =
        new EtcdWatchRetry(
            CONFIG,
            meterRegistry,
            STORE_TAG,
            executor,
            () -> false,
            (revision, unused) -> reopenedAt.add(revision),
            "watch for store /test",
            "cache is stale",
            nowMs::get);
  }

  @AfterEach
  public void tearDown() {
    EtcdMetadataStore.resetFatalErrorHandler();
    executor.shutdownNow();
  }

  // ---------------------------------------------------------------------------------------------
  // Which revision the next attempt resumes from
  // ---------------------------------------------------------------------------------------------

  private record RevisionCase(String name, Throwable error, long expectedRevision) {
    @Override
    public String toString() {
      return name;
    }
  }

  private static List<RevisionCase> revisionCases() {
    return List.of(
        // The regression this policy exists for: a resync here re-listed the whole store, and
        // notified for every node in it, on every retry.
        new RevisionCase(
            "future revision reconnects where the stream got to", FUTURE_REVISION, REACHED),
        new RevisionCase("transient failure reconnects", TRANSIENT, REACHED),
        // Events were genuinely missed, so the captured revision is worthless.
        new RevisionCase("compaction resyncs", COMPACTED, EtcdWatchRetry.REVISION_RESYNC),
        new RevisionCase(
            "an OUT_OF_RANGE that did not say which way resyncs",
            UNCLASSIFIED_REVISION,
            EtcdWatchRetry.REVISION_RESYNC));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("revisionCases")
  public void choosesResumeRevision(RevisionCase testCase) {
    retry.reachedRevision(REACHED);
    retry.onFailure(testCase.error(), "watch failed");

    assertThat(reopenedAt).containsExactly(testCase.expectedRevision());
  }

  /** With no usable revision ever learned, there is nothing to resume from. */
  @Test
  public void resyncsWhenNoRevisionWasEverLearned() {
    retry.requireResync();
    retry.onFailure(TRANSIENT, "revision fetch failed");

    assertThat(reopenedAt).containsExactly(EtcdWatchRetry.REVISION_RESYNC);
  }

  /** The once-per-episode backstop for a revision error the classifier read as reconnectable. */
  @Test
  public void forcesOneResyncAfterRepeatedFailures() {
    retry.reachedRevision(REACHED);
    for (int i = 0; i < 5; i++) {
      flap();
    }

    assertThat(reopenedAt)
        .containsExactly(REACHED, REACHED, EtcdWatchRetry.REVISION_RESYNC, REACHED, REACHED);
  }

  /**
   * A response at a newer revision moves the resume point forward; a stale one cannot drag it back.
   */
  @Test
  public void resumeRevisionOnlyMovesForward() {
    retry.reachedRevision(REACHED);
    retry.reachedRevision(REACHED - 100);
    retry.onFailure(TRANSIENT, "watch failed");

    assertThat(reopenedAt).containsExactly(REACHED);
  }

  /**
   * A watch opened below the revision the stream previously reached — a cluster restored from a
   * snapshot rewinds — resumes from the lower one.
   */
  @Test
  public void aWatchReopenedAtARewoundRevisionAdoptsIt() {
    retry.reachedRevision(REACHED);
    retry.onFailure(COMPACTED, "watch failed");
    // The resync re-listed and found the cluster far behind where the old stream had got to.
    retry.openedAtRevision(REWOUND);
    retry.onFailure(TRANSIENT, "watch failed");

    assertThat(reopenedAt).containsExactly(EtcdWatchRetry.REVISION_RESYNC, REWOUND);
  }

  // ---------------------------------------------------------------------------------------------
  // Pacing
  // ---------------------------------------------------------------------------------------------

  /**
   * A watch accepted and cancelled moments later — the leader-election window — must back off like
   * one never accepted at all.
   */
  @Test
  public void flappingStreamStillEscalates() {
    for (int i = 0; i < ATTEMPTS_TO_CEILING; i++) {
      flap();
    }

    assertThat(executor.delaysMs).hasSize(ATTEMPTS_TO_CEILING);
    assertThat(executor.delaysMs.get(0)).isLessThanOrEqualTo((long) (INITIAL_MS * 1.5));
    assertThat(executor.lastDelayMs()).isGreaterThanOrEqualTo((long) (MAX_MS * JITTER_FLOOR));
  }

  /**
   * A stream that stayed up past the backoff ceiling was a real recovery, so the next failure is a
   * new episode and starts over at the base interval.
   */
  @Test
  public void streamThatStaysUpStartsAFreshEpisode() {
    for (int i = 0; i < ATTEMPTS_TO_CEILING; i++) {
      flap();
    }
    long escalated = executor.lastDelayMs();

    retry.onEstablished();
    nowMs.addAndGet(MAX_MS);
    retry.onFailure(TRANSIENT, "watch failed");

    assertThat(escalated).isGreaterThanOrEqualTo((long) (MAX_MS * JITTER_FLOOR));
    assertThat(executor.lastDelayMs()).isLessThanOrEqualTo((long) (INITIAL_MS * 1.5));
  }

  /** One millisecond short of stable is still a flap, and must not clear the escalation. */
  @Test
  public void streamThatAlmostStaysUpDoesNotResetPacing() {
    for (int i = 0; i < ATTEMPTS_TO_CEILING; i++) {
      flap();
    }

    retry.onEstablished();
    nowMs.addAndGet(MAX_MS - 1);
    retry.onFailure(TRANSIENT, "watch failed");

    assertThat(executor.lastDelayMs()).isGreaterThanOrEqualTo((long) (MAX_MS * JITTER_FLOOR));
  }

  /**
   * Past {@code fatalAfterMs} the watch is unrecoverable, and a flapping stream must reach that
   * threshold like any other.
   */
  @Test
  public void continuousFlappingEventuallyHaltsInsteadOfRetrying() throws InterruptedException {
    CountDownLatch fatal = new CountDownLatch(1);
    EtcdMetadataStore.setFatalErrorHandler(error -> fatal.countDown());

    // Bounded well past the ~61 attempts the thresholds imply, so the loop cannot spin if the
    // policy stops escalating.
    int attempts = 0;
    while (attempts < 200 && fatal.getCount() > 0) {
      flap(MAX_MS - 1);
      attempts++;
    }

    assertThat(fatal.await(5, TimeUnit.SECONDS)).isTrue();
    // A fatal attempt halts, so fewer retries were scheduled than attempts were made.
    assertThat(executor.delaysMs).hasSizeLessThan(attempts);
  }

  /**
   * A watch whose client is closed can never reconnect, so retrying it to {@code fatalAfterMs}
   * would halt a JVM that is already shutting down.
   */
  @Test
  public void aClosedClientStopsTheWatchInsteadOfRetrying() throws InterruptedException {
    CountDownLatch fatal = new CountDownLatch(1);
    EtcdMetadataStore.setFatalErrorHandler(error -> fatal.countDown());

    // Long enough that a retrying watch would have passed fatalAfterMs several times over.
    for (int i = 0; i < 200; i++) {
      retry.onEstablished();
      nowMs.addAndGet(MAX_MS - 1);
      retry.onFailure(CLIENT_CLOSED, "watch failed");
    }

    assertThat(executor.delaysMs).isEmpty();
    assertThat(reopenedAt).isEmpty();
    assertThat(fatal.await(1, TimeUnit.SECONDS)).isFalse();
    assertThat(retryCount("client_closed")).isEqualTo(200);
  }

  // ---------------------------------------------------------------------------------------------
  // Metrics
  // ---------------------------------------------------------------------------------------------

  /**
   * An unrecognized error counts under {@code reason="error"} rather than disappearing into the
   * transient bucket, which is what shows the classifier drifting from etcd's message text.
   */
  @Test
  public void countsRetriesByClassifiedReason() {
    retry.reachedRevision(REACHED);
    retry.onFailure(TRANSIENT, "watch failed");
    retry.onFailure(TRANSIENT, "watch failed");
    retry.onFailure(COMPACTED, "watch failed");
    retry.onFailure(new RuntimeException("something new from etcd"), "watch failed");

    assertThat(retryCount("transient")).isEqualTo(2);
    assertThat(retryCount("compaction")).isEqualTo(1);
    assertThat(retryCount("error")).isEqualTo(1);
  }

  /** The delay distribution is what distinguishes one long episode from many short ones. */
  @Test
  public void recordsTheDelayEachRetryWaited() {
    for (int i = 0; i < ATTEMPTS_TO_CEILING; i++) {
      flap();
    }

    assertThat(
            meterRegistry
                .timer("astra_etcd_watch_retry_delay", "store", STORE_TAG)
                .totalTime(TimeUnit.MILLISECONDS))
        .isEqualTo(executor.delaysMs.stream().mapToLong(Long::longValue).sum());
  }

  private double retryCount(String reason) {
    return meterRegistry
        .counter("astra_etcd_watch_retry", "store", STORE_TAG, "reason", reason)
        .count();
  }

  // ---------------------------------------------------------------------------------------------
  // Callback suppression
  // ---------------------------------------------------------------------------------------------

  /** An error and a completion for the same dead stream must not each drive a recovery. */
  @Test
  public void onlyTheFirstCallbackPerStreamClaimsDisposal() {
    AtomicBoolean disposed = new AtomicBoolean(false);

    assertThat(retry.claimDisposal(disposed, "watch error")).isTrue();
    assertThat(retry.claimDisposal(disposed, "watch completion")).isFalse();
  }

  /** Callbacks fired by our own close, or during shutdown, are not failures to recover from. */
  @Test
  public void suppressedCallbacksNeverClaimDisposal() {
    EtcdWatchRetry closing =
        new EtcdWatchRetry(
            CONFIG,
            meterRegistry,
            STORE_TAG,
            executor,
            () -> true,
            (revision, unused) -> reopenedAt.add(revision),
            "watch for store /test",
            "cache is stale",
            nowMs::get);

    assertThat(closing.claimDisposal(new AtomicBoolean(false), "watch completion")).isFalse();
  }

  /** One turn of a flapping stream: opened, then lost again almost immediately. */
  private void flap() {
    flap(10);
  }

  /** One turn of a flapping stream that stayed open for {@code upForMs} before being lost. */
  private void flap(long upForMs) {
    retry.onEstablished();
    nowMs.addAndGet(upForMs);
    retry.onFailure(TRANSIENT, "watch failed");
  }

  /**
   * Records the delay the policy chose and runs the attempt inline, with a zero-size core pool so
   * no threads are created.
   */
  private static final class RecordingExecutor extends ScheduledThreadPoolExecutor {
    private final List<Long> delaysMs = new ArrayList<>();

    private RecordingExecutor() {
      super(0);
    }

    private long lastDelayMs() {
      return delaysMs.get(delaysMs.size() - 1);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      delaysMs.add(unit.toMillis(delay));
      command.run();
      return null;
    }
  }
}
