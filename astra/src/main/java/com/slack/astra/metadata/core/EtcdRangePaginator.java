package com.slack.astra.metadata.core;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads every key under a folder prefix, paging the etcd range request so no single gRPC response
 * exceeds the client's inbound message size limit (4 MiB default). Pages are read in ascending key
 * order, each after the first pinned to the first page's revision for a consistent snapshot.
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

  /** Reads every key/value under prefix, paging until etcd reports no more keys. */
  static PaginatedRange listRange(
      KV kvClient, ByteSequence prefix, boolean keysOnly, long timeoutMs)
      throws InterruptedException, ExecutionException, TimeoutException {
    List<KeyValue> keyValues = new ArrayList<>();
    ByteSequence fromKey = prefix;
    long revision = REVISION_LATEST;
    GetResponse response;
    do {
      response =
          kvClient
              .get(fromKey, pageOption(prefix, keysOnly, revision))
              .get(timeoutMs, TimeUnit.MILLISECONDS);

      List<KeyValue> page = response.getKvs();
      keyValues.addAll(page);
      // Pin every page after the first to the first page's revision for a consistent snapshot.
      if (revision == REVISION_LATEST) {
        revision = response.getHeader().getRevision();
      }
      // Advance past this page; guard against an empty page before indexing into it.
      if (!page.isEmpty()) {
        fromKey = page.get(page.size() - 1).getKey().concat(NEXT_KEY_SUFFIX);
      }
    } while (response.isMore() && !response.getKvs().isEmpty());
    return new PaginatedRange(keyValues, revision);
  }

  /** Non-blocking variant that runs {@link #listRange} on the common pool. */
  static CompletableFuture<PaginatedRange> listRangeAsync(
      KV kvClient, ByteSequence prefix, boolean keysOnly, long timeoutMs) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return listRange(kvClient, prefix, keysOnly, timeoutMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
          } catch (ExecutionException | TimeoutException e) {
            throw new CompletionException(e);
          }
        });
  }
}
