package com.slack.astra.metadata.core;

import io.etcd.jetcd.common.exception.ClosedClientException;
import io.etcd.jetcd.common.exception.CompactedException;
import io.etcd.jetcd.common.exception.ErrorCode;
import io.etcd.jetcd.common.exception.EtcdException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Classifies an etcd watch failure into the recovery action it requires, matching on error message
 * before error code since compaction and a future revision are both {@code OUT_OF_RANGE}.
 */
final class EtcdErrorClassifier {

  /** Bounds the depth to search. */
  private static final int MAX_CAUSE_DEPTH = 16;

  /** Codes describing the connection or cluster state. */
  private static final Set<Status.Code> TRANSIENT_CODES =
      Set.of(
          Status.Code.UNAVAILABLE,
          Status.Code.INTERNAL,
          Status.Code.CANCELLED,
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.ABORTED,
          Status.Code.RESOURCE_EXHAUSTED);

  /**
   * Retryable conditions etcd reports from the server strings in {@code
   * api/v3rpc/rpctypes/error.go} from etcd server source. This is unfortunately not importable from
   * anywhere.
   */
  private static final List<String> TRANSIENT_MESSAGES =
      List.of(
          "no leader",
          "leader changed",
          "etcdserver: request timed out",
          "etcdserver: too many requests",
          "connection reset",
          "connection closed",
          "goaway");

  /**
   * A closed channel, anchored on a word boundary so gRPC's {@code Subchannel shutdown invoked} —
   * one dropped subchannel under a live client — does not read as one.
   */
  private static final Pattern CHANNEL_SHUTDOWN_MESSAGE =
      Pattern.compile("\\bchannel shutdown invoked");

  /** Message used when the lease no longer exists. */
  private static final List<String> LEASE_GONE_MESSAGES =
      List.of("lease not found", "lease is expired", "ttl not found");

  private EtcdErrorClassifier() {}

  private static String lowerMessage(Throwable error) {
    String message = error == null ? null : error.getMessage();
    return message == null ? "" : message.toLowerCase(Locale.ROOT);
  }

  private static boolean containsAny(String lowerMessage, List<String> needles) {
    return needles.stream().anyMatch(lowerMessage::contains);
  }

  /** The recovery action a watch failure results in. */
  enum Recovery {
    /** Revision was compacted away, so events were missed. Do a resync */
    COMPACTED("compaction"),

    /**
     * Our revision is ahead of the member we reached, or etcd cancelled without a reason; nothing
     * was missed, so this just tries to reconnect.
     */
    FUTURE_REVISION("future_revision"),

    /** Connection or cluster-level failure, reconnect from the last revision we observed. */
    TRANSIENT("transient"),

    /** Unrecognized, recovered like {@link #TRANSIENT} but counted apart so it stays visible. */
    UNKNOWN("error"),

    /** The shared etcd client has been closed, so no reconnect can succeed. */
    CLIENT_CLOSED("client_closed");

    private final String reason;

    Recovery(String reason) {
      this.reason = reason;
    }

    String reason() {
      return reason;
    }

    /**
     * Whether events were lost, forcing a re-list rather than a reconnect. Relisting is more
     * expensive (more pages)
     */
    boolean requiresResync() {
      return this == COMPACTED;
    }

    /** Whether retrying is pointless, so the watch stays down instead of being re-established. */
    boolean isTerminal() {
      return this == CLIENT_CLOSED;
    }
  }

  /**
   * Whether a lease keep-alive failure means the lease itself is gone, which retrying cannot fix.
   */
  static boolean isLeaseGone(Throwable error) {
    if (containsAny(lowerMessage(error), LEASE_GONE_MESSAGES)) {
      return true;
    }
    return error instanceof EtcdException etcdException
        && etcdException.getErrorCode() == ErrorCode.NOT_FOUND;
  }

  /** Classifies a watch failure, walking the cause chain for the most specific match. */
  static Recovery classify(Throwable error) {
    Throwable frame = error;
    for (int depth = 0; frame != null && depth < MAX_CAUSE_DEPTH; depth++) {
      Recovery recovery = classifyFrame(frame);
      if (recovery != null) {
        return recovery;
      }
      frame = frame.getCause();
    }
    return Recovery.UNKNOWN;
  }

  /** The classification for a single frame, or null if the frame isn't complete/missing info. */
  private static Recovery classifyFrame(Throwable frame) {
    if (frame instanceof ClosedClientException || frame instanceof RejectedExecutionException) {
      return Recovery.CLIENT_CLOSED;
    }

    if (frame instanceof CompactedException) {
      return Recovery.COMPACTED;
    }

    String lower = lowerMessage(frame);
    if (CHANNEL_SHUTDOWN_MESSAGE.matcher(lower).find()) {
      return Recovery.CLIENT_CLOSED;
    }
    if (lower.contains("compacted")) {
      return Recovery.COMPACTED;
    }
    if (lower.contains("future revision")) {
      return Recovery.FUTURE_REVISION;
    }

    Status.Code code = codeOf(frame);
    if (code == Status.Code.OUT_OF_RANGE) {
      // The message did not say which way; resyncing works whether we are behind or ahead.
      return Recovery.COMPACTED;
    }
    if (code != null && TRANSIENT_CODES.contains(code)) {
      return Recovery.TRANSIENT;
    }
    if (frame instanceof TimeoutException) {
      return Recovery.TRANSIENT;
    }
    if (containsAny(lower, TRANSIENT_MESSAGES)) {
      return Recovery.TRANSIENT;
    }
    return null;
  }

  /**
   * The status code a frame carries, or null; etcd's {@link ErrorCode} is {@link Status.Code} minus
   * {@code OK}, so every name it produces is a valid one.
   */
  private static Status.Code codeOf(Throwable frame) {
    if (frame instanceof EtcdException etcdException) {
      return Status.Code.valueOf(etcdException.getErrorCode().name());
    }
    if (frame instanceof StatusRuntimeException statusException) {
      return statusException.getStatus().getCode();
    }
    return null;
  }
}
