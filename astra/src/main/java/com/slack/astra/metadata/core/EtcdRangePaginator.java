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
 * Reads every key under a folder prefix, paging the underlying etcd range request so that no single
 * gRPC response can exceed the client's inbound message size limit (4 MiB by default). A single
 * unbounded range read over a large store previously overflowed this limit and failed with {@code
 * RESOURCE_EXHAUSTED} (e.g. a query node loading the search metadata cache, or a partitioning store
 * enumerating partitions in a very large cluster).
 *
 * <p>Pages are read in ascending key order and advanced past the last key of the previous page.
 * Every page after the first is pinned to the revision of the first page, so the aggregated result
 * is a consistent point-in-time snapshot. Both a blocking ({@link #listRange}) and a non-blocking
 * ({@link #listRangeAsync}) variant are provided; each page is individually bounded by {@code
 * timeoutMs}.
 */
final class EtcdRangePaginator {
  /** Sentinel meaning "read at the latest revision"; etcd treats revision 0 as latest. */
  private static final long REVISION_LATEST = 0;

  /**
   * Maximum number of entries fetched per etcd range request. Bounds a page by entry count rather
   * than bytes (etcd's range API has no byte budget), so it protects the default 4 MiB limit only
   * while per-entry size stays under ~8 KiB — comfortably true for the metadata types stored here.
   */
  static final long DEFAULT_PAGE_SIZE = 500;

  /** Single zero byte appended to a key to form the smallest key strictly greater than it. */
  private static final ByteSequence NEXT_KEY_SUFFIX = ByteSequence.from(new byte[] {0});

  private EtcdRangePaginator() {}

  /**
   * The key/values read under a folder prefix, together with the etcd revision the read was pinned
   * to. Callers that establish a watch use {@link #revision()} to start watching from {@code
   * revision + 1} without missing or replaying events.
   */
  record PaginatedRange(List<KeyValue> keyValues, long revision) {}

  /**
   * Appends a single zero byte to a key to produce the smallest key strictly greater than it, used
   * to advance a range read past the last key of the previous page without skipping or repeating
   * any key.
   */
  private static ByteSequence nextKey(ByteSequence key) {
    return key.concat(NEXT_KEY_SUFFIX);
  }

  /**
   * Builds the {@link GetOption} for a single page: key-ascending order, bounded to {@link
   * #DEFAULT_PAGE_SIZE} entries. When {@code revision} is past {@link #REVISION_LATEST} the read is
   * pinned to that revision so every page observes one consistent snapshot.
   */
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

  /**
   * Reads every key/value under {@code prefix} synchronously, one page at a time.
   *
   * @param kvClient the etcd KV client to read through
   * @param prefix the folder prefix (including any trailing slash) to range over
   * @param keysOnly when true, only key metadata is fetched and values are omitted
   * @param timeoutMs per-page operation timeout
   * @return the aggregated key/values and the revision they were read at
   */
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
