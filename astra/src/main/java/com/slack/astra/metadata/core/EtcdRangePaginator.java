package com.slack.astra.metadata.core;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.options.GetOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Pages are read in ascending key order, each after the first pinned to the first page's revision
 * for a consistent snapshot.
 */
final class EtcdRangePaginator {
  /** etcd treats revision 0 as latest. */
  private static final long REVISION_LATEST = 0;

  static final long DEFAULT_PAGE_SIZE = 500;

  /**
   * Appended to a page's last key to resume the next page. etcd keys are byte strings sorted in
   * lexical byte order, and a range read's start key is inclusive, so resuming from {@code lastKey
   * + \0} — the smallest key that sorts after {@code lastKey} — reads the next key without
   * repeating the last one. See the etcd data model for key ordering
   * (https://etcd.io/docs/v3.5/learning/data_model/); the {@code lastKey + "\x00"} resume pattern
   * matches the Kubernetes apiserver etcd3 store
   * (https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go).
   */
  private static final ByteSequence NEXT_KEY_SUFFIX = ByteSequence.from(new byte[] {0});

  private EtcdRangePaginator() {}

  /** The key/values read under a prefix, with the revision the read was pinned to. */
  record PaginatedRange(List<KeyValue> keyValues, long revision) {}

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

  static CompletableFuture<PaginatedRange> listRangeAsync(
      KV kvClient, ByteSequence prefix, boolean keysOnly, long timeoutMs) {
    return fetchPage(
        kvClient, prefix, prefix, keysOnly, timeoutMs, REVISION_LATEST, new ArrayList<>());
  }

  private static CompletableFuture<PaginatedRange> fetchPage(
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
              // Pin every page after the first to the first page's revision for a consistent
              // snapshot.
              long revision =
                  pinnedRevision > REVISION_LATEST
                      ? pinnedRevision
                      : response.getHeader().getRevision();

              List<KeyValue> page = response.getKvs();
              keyValues.addAll(page);
              if (!response.isMore() || page.isEmpty()) {
                return CompletableFuture.completedFuture(new PaginatedRange(keyValues, revision));
              }

              ByteSequence nextKey = page.get(page.size() - 1).getKey().concat(NEXT_KEY_SUFFIX);
              return fetchPage(kvClient, prefix, nextKey, keysOnly, timeoutMs, revision, keyValues);
            });
  }

  static PaginatedRange listRange(
      KV kvClient, ByteSequence prefix, boolean keysOnly, long timeoutMs)
      throws InterruptedException, ExecutionException, TimeoutException {
    return listRangeAsync(kvClient, prefix, keysOnly, timeoutMs)
        .get(timeoutMs, TimeUnit.MILLISECONDS);
  }
}
