package com.slack.kaldb.util;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.kaldb.util.FutureUtils.successCountingCallback;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A metadata class that contains useful utilities for working with Snapshots. */
public class SnapshotsUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotsUtil.class);
  private static final int SNAPSHOT_OPERATION_TIMEOUT_SECS = 10;

  public static boolean deleteSnapshots(
      SnapshotMetadataStore snapshotMetadataStore, List<SnapshotMetadata> snapshots) {
    LOG.info("Deleting {} snapshots: {}", snapshots.size(), snapshots);

    AtomicInteger successCounter = new AtomicInteger(0);
    List<? extends ListenableFuture<?>> deletionFutures =
        snapshots
            .stream()
            .map(
                snapshot -> {
                  ListenableFuture<?> future = snapshotMetadataStore.delete(snapshot);
                  addCallback(
                      future,
                      successCountingCallback(successCounter),
                      MoreExecutors.directExecutor());
                  return future;
                })
            .collect(Collectors.toUnmodifiableList());

    ListenableFuture<?> futureList = Futures.successfulAsList(deletionFutures);
    try {
      futureList.get(SNAPSHOT_OPERATION_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (Exception e) {
      futureList.cancel(true);
    }

    final int success = successCounter.get();
    if (success == snapshots.size()) {
      LOG.info("Successfully deleted all {} snapshots.", success);
      return true;
    } else {
      LOG.warn(
          "Failed to delete {} snapshots within {} secs.",
          SNAPSHOT_OPERATION_TIMEOUT_SECS,
          snapshots.size() - success);
      return false;
    }
  }
}
