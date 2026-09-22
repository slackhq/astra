package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.metadata.core.EtcdErrorClassifier.Recovery;
import io.etcd.jetcd.common.exception.ErrorCode;
import io.etcd.jetcd.common.exception.EtcdExceptionFactory;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies watch-error classification against exceptions built the way jetcd builds them, cases
 * marked "as jetcd raises it" using the {@link EtcdExceptionFactory} that {@code WatchImpl} calls.
 */
public class EtcdErrorClassifierTest {

  private record Case(String name, Throwable error, Recovery expected) {
    @Override
    public String toString() {
      return name;
    }
  }

  /** Mirrors {@code EtcdErrorClassifier.TRANSIENT_CODES}; the two must move together. */
  private static final List<Status.Code> TRANSIENT_STATUS_CODES =
      List.of(
          Status.Code.UNAVAILABLE,
          Status.Code.INTERNAL,
          Status.Code.CANCELLED,
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.ABORTED,
          Status.Code.RESOURCE_EXHAUSTED);

  private static List<Case> cases() {
    List<Case> cases = new ArrayList<>();
    cases.addAll(handWrittenCases());
    TRANSIENT_STATUS_CODES.forEach(
        code ->
            cases.add(
                new Case(
                    code + " code maps to transient",
                    EtcdExceptionFactory.newEtcdException(
                        ErrorCode.valueOf(code.name()), code + " failure"),
                    Recovery.TRANSIENT)));
    return cases;
  }

  private static List<Case> handWrittenCases() {
    return List.of(
        // Compaction: our revision aged out, events were missed, the cache must be re-listed.
        new Case(
            "compaction as jetcd raises it",
            EtcdExceptionFactory.newCompactedException(42),
            Recovery.COMPACTED),
        new Case(
            "compaction by message",
            new RuntimeException("etcd revision has been compacted"),
            Recovery.COMPACTED),
        new Case(
            "compaction wrapped in a generic cause chain",
            new IllegalStateException(
                "watch failed", EtcdExceptionFactory.newCompactedException(7)),
            Recovery.COMPACTED),

        // Future revision: shares OUT_OF_RANGE with compaction, so only the message separates them.
        new Case(
            "future revision as jetcd raises it",
            EtcdExceptionFactory.newEtcdException(
                ErrorCode.OUT_OF_RANGE, "etcdserver: mvcc: required revision is a future revision"),
            Recovery.FUTURE_REVISION),

        // OUT_OF_RANGE that did not say which way: could be compaction, so it must resync.
        new Case(
            "OUT_OF_RANGE with no distinguishing message",
            EtcdExceptionFactory.newEtcdException(ErrorCode.OUT_OF_RANGE, null),
            Recovery.COMPACTED),
        new Case(
            "OUT_OF_RANGE from a gRPC status",
            new StatusRuntimeException(Status.OUT_OF_RANGE),
            Recovery.COMPACTED),

        // Transient: reconnect at the revision we already observed.
        new Case(
            "GOAWAY from a node leaving cleanly",
            EtcdExceptionFactory.newEtcdException(
                ErrorCode.UNAVAILABLE,
                "Connection closed after GOAWAY. HTTP/2 error code: NO_ERROR, debug data: graceful_stop"),
            Recovery.TRANSIENT),
        new Case(
            "GOAWAY carrying no status code at all, matched by message",
            new RuntimeException(
                "Connection closed after GOAWAY. HTTP/2 error code: NO_ERROR, debug data: something_else"),
            Recovery.TRANSIENT),
        new Case(
            "leader change as jetcd raises it",
            EtcdExceptionFactory.toEtcdException(
                Status.UNAVAILABLE.withDescription("etcdserver: leader changed")),
            Recovery.TRANSIENT),
        new Case(
            "no leader during an election",
            EtcdExceptionFactory.toEtcdException(
                Status.UNAVAILABLE.withDescription("etcdserver: no leader")),
            Recovery.TRANSIENT),
        new Case(
            "UNAVAILABLE with a null description",
            EtcdExceptionFactory.newEtcdException(ErrorCode.UNAVAILABLE, null),
            Recovery.TRANSIENT),
        new Case(
            "a transient code from a gRPC status rather than an EtcdException",
            new StatusRuntimeException(Status.RESOURCE_EXHAUSTED),
            Recovery.TRANSIENT),
        new Case("timeout awaiting a response", new TimeoutException(), Recovery.TRANSIENT),
        new Case(
            "connection reset with no status attached",
            new RuntimeException("Connection reset by peer"),
            Recovery.TRANSIENT),

        // Client closed: retrying cannot succeed, and each of these reads as transient on its code
        // alone — ClosedClientException carries CANCELLED, a dead channel reports UNAVAILABLE.
        new Case(
            "closed watch client as jetcd raises it",
            EtcdExceptionFactory.newClosedWatchClientException(),
            Recovery.CLIENT_CLOSED),
        new Case(
            "closed lease client as jetcd raises it",
            EtcdExceptionFactory.newClosedLeaseClientException(),
            Recovery.CLIENT_CLOSED),
        new Case(
            "a call on a channel the client already shut down",
            EtcdExceptionFactory.toEtcdException(
                Status.UNAVAILABLE.withDescription("Channel shutdown invoked")),
            Recovery.CLIENT_CLOSED),
        new Case(
            "a call on a subchannel the client already shut down",
            new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("Subchannel shutdown invoked")),
            Recovery.CLIENT_CLOSED),
        new Case(
            "work rejected by the client's terminated executor",
            new ExecutionException(
                new RejectedExecutionException("rejected from ThreadPoolExecutor[Terminated]")),
            Recovery.CLIENT_CLOSED),

        // Unrecognized: still recovered, but counted apart so it stays visible.
        new Case(
            "unrecognized error", new RuntimeException("connection refused"), Recovery.UNKNOWN),
        new Case("null message", new RuntimeException((String) null), Recovery.UNKNOWN),
        new Case(
            "FAILED_PRECONDITION cancel reason",
            EtcdExceptionFactory.newEtcdException(ErrorCode.FAILED_PRECONDITION, "some reason"),
            Recovery.UNKNOWN));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  public void classifiesWatchErrors(Case testCase) {
    assertThat(EtcdErrorClassifier.classify(testCase.error())).isEqualTo(testCase.expected());
  }

  /**
   * Only a failure implying missed events may discard the captured revision; a resync costs a full
   * re-list plus a notification per node.
   */
  @Test
  public void onlyMissedEventsRequireResync() {
    assertThat(Recovery.COMPACTED.requiresResync()).isTrue();
    assertThat(Recovery.FUTURE_REVISION.requiresResync()).isFalse();
    assertThat(Recovery.TRANSIENT.requiresResync()).isFalse();
    assertThat(Recovery.UNKNOWN.requiresResync()).isFalse();
  }

  /** Only a closed client is unrecoverable; every other failure is worth another attempt. */
  @Test
  public void onlyAClosedClientIsTerminal() {
    assertThat(Recovery.CLIENT_CLOSED.isTerminal()).isTrue();
    assertThat(Recovery.COMPACTED.isTerminal()).isFalse();
    assertThat(Recovery.FUTURE_REVISION.isTerminal()).isFalse();
    assertThat(Recovery.TRANSIENT.isTerminal()).isFalse();
    assertThat(Recovery.UNKNOWN.isTerminal()).isFalse();
  }

  /** A cyclic cause chain must terminate rather than spin. */
  @Test
  public void cyclicCauseChainTerminates() {
    RuntimeException first = new RuntimeException("unhelpful");
    RuntimeException second = new RuntimeException("also unhelpful");
    first.initCause(second);
    second.initCause(first);
    assertThat(EtcdErrorClassifier.classify(first)).isEqualTo(Recovery.UNKNOWN);
  }

  @Test
  public void nullErrorIsUnknown() {
    assertThat(EtcdErrorClassifier.classify(null)).isEqualTo(Recovery.UNKNOWN);
  }
}
