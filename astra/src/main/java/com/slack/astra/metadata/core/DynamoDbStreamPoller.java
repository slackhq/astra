package com.slack.astra.metadata.core;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

/**
 * A deliberately-simple DynamoDB Streams consumer for the metadata-store POC.
 *
 * <p>It discovers the table's stream, obtains a {@code LATEST} iterator per shard synchronously at
 * {@link #start()} (so no change that happens after construction is missed), then polls {@code
 * GetRecords} on a single background thread and hands each matching change to the supplied handler.
 * Ordering is preserved by the single-thread poll loop, mirroring {@link EtcdMetadataStore}'s
 * single {@code WATCH_EVENT_EXECUTOR}.
 *
 * <p>This is NOT production-grade: a real consumer would use the Kinesis Adapter + KCL for sharded,
 * checkpointed, multi-consumer streams. The POC uses one consumer, no lease table, and re-scans the
 * shard list each poll to pick up splits.
 */
class DynamoDbStreamPoller implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoDbStreamPoller.class);
  private static final long POLL_INTERVAL_MS = 300;

  /**
   * A single change scoped past the {@code pkFilter}. For an upsert {@code image} is the new image;
   * for a removal it is the old image (may be empty if the stream view lacks it).
   */
  record Change(boolean removed, String pk, String sk, Map<String, AttributeValue> image) {}

  private final DynamoDbStreamsAsyncClient streamsClient;
  private final DynamoDbAsyncClient ddbClient;
  private final String tableName;
  private final Predicate<String> pkFilter;
  private final Consumer<Change> handler;
  private final long timeoutMs;

  private final ScheduledExecutorService executor;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final Map<String, String> shardIterators = new ConcurrentHashMap<>();
  private volatile String streamArn;

  DynamoDbStreamPoller(
      DynamoDbStreamsAsyncClient streamsClient,
      DynamoDbAsyncClient ddbClient,
      String tableName,
      Predicate<String> pkFilter,
      Consumer<Change> handler,
      long timeoutMs) {
    this.streamsClient = streamsClient;
    this.ddbClient = ddbClient;
    this.tableName = tableName;
    this.pkFilter = pkFilter;
    this.handler = handler;
    this.timeoutMs = timeoutMs;
    ThreadFactory factory =
        r -> {
          Thread t = new Thread(r, "dynamodb-stream-poller-" + tableName);
          t.setDaemon(true);
          return t;
        };
    this.executor = Executors.newSingleThreadScheduledExecutor(factory);
  }

  void start() {
    try {
      streamArn =
          ddbClient
              .describeTable(b -> b.tableName(tableName))
              .get(timeoutMs, TimeUnit.MILLISECONDS)
              .table()
              .latestStreamArn();
      if (streamArn == null) {
        throw new InternalMetadataStoreException(
            "Table " + tableName + " has no stream enabled; cannot watch");
      }
      // LATEST so we only see changes after construction; the initial cache Query already
      // captured everything before this point.
      refreshShardIterators(ShardIteratorType.LATEST);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InternalMetadataStoreException("Interrupted starting stream poller", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Failed to start stream poller", e);
    }
    running.set(true);
    executor.scheduleWithFixedDelay(this::poll, 0, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  private void refreshShardIterators(ShardIteratorType type) {
    try {
      List<Shard> shards =
          streamsClient
              .describeStream(b -> b.streamArn(streamArn))
              .get(timeoutMs, TimeUnit.MILLISECONDS)
              .streamDescription()
              .shards();
      for (Shard shard : shards) {
        shardIterators.computeIfAbsent(shard.shardId(), id -> shardIterator(id, type));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException | TimeoutException e) {
      LOG.warn("Failed to refresh stream shards for {}", tableName, e);
    }
  }

  private String shardIterator(String shardId, ShardIteratorType type) {
    try {
      return streamsClient
          .getShardIterator(b -> b.streamArn(streamArn).shardId(shardId).shardIteratorType(type))
          .get(timeoutMs, TimeUnit.MILLISECONDS)
          .shardIterator();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (ExecutionException | TimeoutException e) {
      LOG.warn("Failed to get shard iterator for {}", shardId, e);
      return null;
    }
  }

  private void poll() {
    if (!running.get()) {
      return;
    }
    try {
      // Pick up new shards created by splits. TRIM_HORIZON so we don't miss a new shard's start.
      refreshShardIterators(ShardIteratorType.TRIM_HORIZON);
      for (Map.Entry<String, String> entry : shardIterators.entrySet()) {
        String iterator = entry.getValue();
        if (iterator == null) {
          continue;
        }
        var response =
            streamsClient
                .getRecords(b -> b.shardIterator(iterator))
                .get(timeoutMs, TimeUnit.MILLISECONDS);
        response.records().forEach(this::dispatch);
        String next = response.nextShardIterator();
        if (next == null) {
          shardIterators.remove(entry.getKey()); // shard closed
        } else {
          shardIterators.put(entry.getKey(), next);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOG.warn("Error polling DynamoDB stream for {}", tableName, e);
    }
  }

  private void dispatch(software.amazon.awssdk.services.dynamodb.model.Record record) {
    StreamRecord sr = record.dynamodb();
    Map<String, AttributeValue> keys = sr.keys();
    AttributeValue pkAttr = keys.get(DynamoDbMetadataStore.PK);
    AttributeValue skAttr = keys.get(DynamoDbMetadataStore.SK);
    if (pkAttr == null || skAttr == null) {
      return;
    }
    String pk = pkAttr.s();
    if (!pkFilter.test(pk)) {
      return;
    }
    boolean removed = record.eventName() == OperationType.REMOVE;
    Map<String, AttributeValue> image = removed ? sr.oldImage() : sr.newImage();
    try {
      handler.accept(new Change(removed, pk, skAttr.s(), image));
    } catch (Exception e) {
      LOG.warn("Stream change handler threw for {}/{}", pk, skAttr.s(), e);
    }
  }

  @Override
  public void close() {
    running.set(false);
    executor.shutdownNow();
  }
}
