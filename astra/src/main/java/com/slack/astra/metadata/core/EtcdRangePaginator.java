package com.slack.astra.metadata.core;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads every key under a folder prefix, paging the etcd range request so no single gRPC response
 * exceeds the client's inbound message size limit (4 MiB default). Pages are read in ascending key
 * order, each after the first pinned to the first page's revision for a consistent snapshot.
 */
final class EtcdRangePaginator {
  /** Sentinel meaning "read at the latest revision"; etcd treats revision 0 as latest. */
  private static final long REVISION_LATEST = 0;

  /** Max entries per page. Protects the 4 MiB limit while per-entry size stays under ~8 KiB. */
  static final long DEFAULT_PAGE_SIZE = 500;

  /** Single zero byte appended to a key to form the smallest key strictly greater than it. */
  private static final ByteSequence NEXT_KEY_SUFFIX = ByteSequence.from(new byte[] {0});

  private EtcdRangePaginator() {}

  /** The key/values read under a prefix, with the revision the read was pinned to. */
  record PaginatedRange(List<KeyValue> keyValues, long revision) {}

  /** Smallest key strictly greater than {@code key}, used to advance past the previous page. */
  private static ByteSequence nextKey(ByteSequence key) {
    return key.concat(NEXT_KEY_SUFFIX);
  }

  private static GetOption pageOption(ByteSequence prefix, boolean keysOnly, long revision) {
    GetOption.Builder options =
        GetOption.builder()
            .withPrefix(prefix)
            .withLimit(DEFAULT_PAGE_SIZE)
            .withSortField(GetOption.SortTarget.KEY)
            .withSortOrder(GetOption.SortOrder.ASCEND)
            .withKeysOnly(keysOnly);
    if (revision > REVISION_LATEST) {
      options.withRevision(revision);
    }
    return options.build();
  }

  /** Reads every key/value under {@code prefix} synchronously, one page at a time. */
  static PaginatedRange listRange(
      KV kvClient, ByteSequence prefix, boolean keysOnly, long timeoutMs)
      throws InterruptedException, ExecutionException, TimeoutException {
    List<KeyValue> keyValues = new ArrayList<>();
    ByteSequence fromKey = prefix;
    long revision = REVISION_LATEST;
    while (true) {
      GetResponse response =
          kvClient
              .get(fromKey, pageOption(prefix, keysOnly, revision))
              .get(timeoutMs, TimeUnit.MILLISECONDS);

      List<KeyValue> page = response.getKvs();
      keyValues.addAll(page);
      if (revision == REVISION_LATEST) {
        revision = response.getHeader().getRevision();
      }

      if (!response.isMore() || page.isEmpty()) {
        return new PaginatedRange(keyValues, revision);
      }
      fromKey = nextKey(page.get(page.size() - 1).getKey());
    }
  }

  /** Non-blocking variant of {@link #listRange}; pages are chained sequentially via futures. */
  static CompletableFuture<PaginatedRange> listRangeAsync(
      KV kvClient, ByteSequence prefix, boolean keysOnly, long timeoutMs) {
    return fetchPageAsync(
        kvClient, prefix, prefix, keysOnly, timeoutMs, REVISION_LATEST, new ArrayList<>());
  }

  private static CompletableFuture<PaginatedRange> fetchPageAsync(
      KV kvClient,
      ByteSequence prefix,
      ByteSequence fromKey,
      boolean keysOnly,
      long timeoutMs,
      long pinnedRevision,
      List<KeyValue> keyValues) {
    return kvClient
        .get(fromKey, pageOption(prefix, keysOnly, pinnedRevision))
        .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
        .thenCompose(
            response -> {
              List<KeyValue> page = response.getKvs();
              keyValues.addAll(page);
              long revision =
                  pinnedRevision > REVISION_LATEST
                      ? pinnedRevision
                      : response.getHeader().getRevision();
              if (!response.isMore() || page.isEmpty()) {
                return CompletableFuture.completedFuture(new PaginatedRange(keyValues, revision));
              }
              return fetchPageAsync(
                  kvClient,
                  prefix,
                  nextKey(page.get(page.size() - 1).getKey()),
                  keysOnly,
                  timeoutMs,
                  revision,
                  keyValues);
            });
  }
}
