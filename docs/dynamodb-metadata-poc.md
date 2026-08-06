# POC: DynamoDB as an Astra Metadata Store Backend

## Context

Astra stores all cluster metadata (datasets, snapshots, replicas, cache-slot
assignments, search/recovery node liveness, etc.) in a metadata store. Today
that store has exactly **two** backends — ZooKeeper (via Apache Curator) and
etcd (via jetcd). This POC evaluates **DynamoDB** as a managed,
operationally-cheaper alternative: no ZK/etcd clusters to run, and it leverages
the AWS footprint Astra already uses for S3.

This is a **proof-of-concept**, not a migration. The goal is to answer one
question with running code: *can a DynamoDB-backed store satisfy the semantics
Astra's metadata layer actually requires* — create-if-not-exists,
get/update/delete, list, **watch/subscribe** (via DynamoDB Streams), and
**ephemeral liveness nodes** (via DynamoDB TTL)? We prove this in a **vertical
slice** — a self-contained `DynamoDbMetadataStore<T>` exercised directly by
tests against a local DynamoDB — without rewiring Astra's two-backend bridge.

### Slice targets

The backend under evaluation is **DynamoDB**, proven across two types that
together cover the full semantic surface:

1. **`DatasetMetadata` / `DatasetMetadataStore`** — a **persistent,
   non-partitioned** type. Establishes baseline CRUD + list + watch.
2. **`SearchMetadata` / `SearchMetadataStore`** — an **ephemeral, partitioned**
   type (`AstraPartitioningMetadataStore` + `CreateMode.EPHEMERAL`, store path
   `/partitioned_search`, `getPartition()` = `snapshotName`). This is **the
   hardest and most operationally troublesome case today**: per-search-node
   registration that must appear on startup and *reliably vanish when the node
   dies*, sharded by partition. If TTL-based liveness + Streams-based watch hold
   up for the ephemeral-partitioned store, the rest is easy by comparison.

Both types' existing models + serializers are reused unchanged.

### Why the slice is standalone

The metadata layer is **not** a pluggable SPI. `AstraMetadataStore<T>` and
`AstraPartitioningMetadataStore<T>`
(`astra/src/main/java/com/slack/astra/metadata/core/`) are a hardcoded
**bridge** holding concrete `zkStore` + `etcdStore` fields and dispatching every
operation through `switch (mode)` on the `MetadataStoreMode` enum. There are
~96 `case` sites across those two classes, and 13 concrete stores each construct
*both* sub-stores in their constructors. Wiring a third backend into that bridge
is a large, invasive change and is **out of scope** for the POC. Instead we
mirror the *shape* of `EtcdMetadataStore` in new standalone classes and validate
them in isolation.

## Approach

Build a standalone `DynamoDbMetadataStore<T extends AstraMetadata>` (plus a
`DynamoDbPartitioningMetadataStore<T extends AstraPartitionedMetadata>`) that
mirrors the public method surface of `EtcdMetadataStore` — the newest, most
self-contained backend and the natural template. Reuse the existing
serialization and model base classes unchanged. Prove it with integration tests
against **DynamoDB Local** (Testcontainers).

### Data model (single table)

One DynamoDB table (e.g. `astra_metadata`), single-table style. The key schema
is chosen so that the **partitioned** store maps onto DynamoDB natively:

- **Partition key** `pk` (String):
  - Non-partitioned store: the logical store folder (e.g. `/service`).
  - Partitioned store: `storeFolder + "/" + partitionId` (e.g.
    `/partitioned_search/{snapshotName}`). A per-partition `list()` / watch then
    becomes a single DynamoDB `Query` on `pk` — exactly the scaling problem
    partitioning exists to solve. A full cross-partition `listSync` is a `Scan`
    (POC-acceptable; cost documented). Note the legacy bridge's
    partition-unaware `find(name)` is **not** carried into the POC (see below).
- **Sort key** `sk` (String) = the node name (`AstraMetadata#getName()`).
- Attribute `payload` (String) = the protobuf-as-JSON produced by the existing
  `MetadataSerializer<T>` — reused as-is, no new serialization code.
- Attribute `ttl` (Number, epoch seconds) — present only for ephemeral nodes;
  the table's TTL is configured on this attribute.
- DynamoDB **Streams** enabled on the table (`NEW_AND_OLD_IMAGES`) to drive
  watch/cache.

### Operation mapping (mirror the etcd semantics)

| Store op | DynamoDB implementation |
|---|---|
| `createAsync/Sync` | `PutItem` with `ConditionExpression="attribute_not_exists(sk)"` → conditional-check-failure maps to the "already exists" error. |
| `getAsync/Sync` | `GetItem`; missing item → `InternalMetadataStoreException` (get never returns null). |
| `hasAsync/Sync` | `GetItem` (projection on `sk`) → boolean; never throws on absent. |
| `updateAsync/Sync` | `PutItem` (overwrite), optionally conditional on `attribute_exists`. |
| `deleteAsync/Sync` | `DeleteItem`. |
| `listAsync/Sync` | `Query` on `pk` (paginated) → deserialize each `payload`; reads cache when caching is enabled. |
| `addListener/removeListener` + cache | DynamoDB Streams consumer (below). |
| `awaitCacheInitialized` | `CountDownLatch` released after the initial `Query` populates the cache. |
| ephemeral create | `PutItem` with `ttl` = now + TTL; a background heartbeat bumps `ttl`, mirroring etcd's shared-lease keepalive. |

### Watch via DynamoDB Streams

Mirror `EtcdMetadataStore`'s watch design (single-thread ordered dispatch,
in-memory `ConcurrentHashMap<String,T>` cache, listener fan-out):

- On construction (when caching), do the initial `Query` to populate the cache,
  record the starting position, then start a poller.
- A background poller reads the table's Stream shards (`DescribeStream` →
  `GetShardIterator` → `GetRecords` loop). `INSERT`/`MODIFY` → deserialize new
  image → `cache.put` + notify; `REMOVE` (including TTL expiry, which appears
  with `userIdentity.principalId = dynamodb.amazonaws.com`) → `cache.remove` +
  notify. Dispatch on a single-thread executor to preserve ordering.
- The POC keeps the poller simple (single consumer, no KCL/lease table). A
  production version would use the Kinesis Adapter + KCL for sharded,
  checkpointed, multi-consumer streams.

### Ephemeral nodes via TTL

- Ephemeral `create` writes a `ttl` epoch-seconds attribute; a scheduled
  heartbeat bumps it, analogous to the etcd lease keepalive.
- **POC caveat (key risk):** DynamoDB TTL deletion is *best-effort* (can lag up
  to ~48h) and DynamoDB Local does not run the TTL sweeper at all. So the read
  path **filters out logically-expired items** (`ttl < now`) on
  `get`/`has`/`list`/cache-load to get ZK/etcd-like liveness semantics; the
  physical delete is background cleanup, not the source of truth. This gap
  (best-effort expiry vs. etcd's second-level lease revocation) is the single
  most important thing to resolve before any real migration.

## Risks / open questions

1. **Watch latency & consumer complexity** — Streams polling is higher-latency
   than native etcd/ZK watch, and a correct production consumer (sharding,
   checkpointing, multi-node) is non-trivial.
2. **Best-effort TTL vs. lease revocation** — liveness correctness depends on
   the read-path expiry filter, not on DynamoDB actually deleting on time.
3. **Cross-partition scans** — a full cross-partition `listSync` is a `Scan`;
   fine for the POC, needs a GSI or redesign at scale.

## Access model: no partition-unaware `find`

The legacy two-backend bridge exposes a partition-unaware `find(name)` that
locates a node without knowing its partition — on ZK/etcd it loops `has(name)`
over every partition, and a naive DynamoDB port would be a full-table `Scan` on
the `sk` (node name) with no usable index. The POC **deliberately omits it**.
All point access is partition-aware — `getAsync(partition, path)` /
`hasAsync(partition, path)` / `listSync(partition)` — which folds the partition
into the `pk` and maps to a native single-partition `GetItem`/`Query`, i.e.
Dynamo's model. This is safe because `find` has no production callers: its one
historical caller (`CacheNodeAssignmentService`) already removed its `findSync`
(commit `2dadb2e3`), hoisting resolution into a caller-supplied map rather than
scanning partitions. Consequently the POC needs no GSI on `sk`.

## Local selection: wiring a vertical slice (implemented)

To exercise the backend inside a running Astra (e.g. the local kind harness), two
stores — `DatasetMetadataStore` (persistent, non-partitioned) and
`SearchMetadataStore` (ephemeral, partitioned) — can now be **configured** onto
DynamoDB, without touching the other 11 stores:

- `MetadataStoreMode.DYNAMODB_CREATES` added to the proto enum.
- The two bridge classes (`AstraMetadataStore`, `AstraPartitioningMetadataStore`)
  gained a nullable `dynamoStore` field and an **exclusive delegate**: each public
  op early-returns `dynamoStore.op(...)` when the mode is `DYNAMODB_CREATES`,
  leaving the pairwise ZK/etcd fallback/merge/dual-delete logic byte-for-byte
  unchanged. `addListener`/`removeListener`/`awaitCacheInitialized` route to the
  Dynamo store only; `close()` also closes it. The partition-unaware `find*` arms
  throw `UnsupportedOperationException` (see access model above).
- `Astra.java#start()` builds the `DynamoDbAsyncClient` /
  `DynamoDbStreamsAsyncClient` (gated on `dynamodbConfig.enabled`) and threads
  them into **only** the two sliced store constructors; both are closed on
  shutdown. A dynamo-only `SearchMetadataStore` skips its ZK/etcd legacy
  `/search` fallback store.
- `config/config.yaml` / `test_config.yaml` carry a `dynamodbConfig:` block with
  `${ASTRA_DYNAMODB_*}` env overrides. Select a store by setting its
  `*_METADATA_STORE_MODE=DYNAMODB_CREATES` and `ASTRA_DYNAMODB_ENABLED=true`.
  `ValidateAstraConfig` requires an enabled config + table name when any store
  uses `DYNAMODB_CREATES`.
- `DynamoDbBridgeWiringTest` proves config-driven selection end-to-end against
  DynamoDB Local (no kind cluster needed).

## Out of scope (follow-up if the POC succeeds)

- Extending `DYNAMODB_CREATES` to the other 11 concrete stores and their
  `getServices` call sites (only the two sliced stores are wired).
- Production Streams consumption (Kinesis Adapter + KCL) and a durable
  ephemeral-expiry strategy that doesn't depend on best-effort TTL.

## Verification

1. Build: `cd astra && mvn clean install -DskipTests`.
2. Confirm `com.slack.astra.proto.config.AstraConfigs.DynamoDbConfig` generates.
3. Run the POC tests (Docker must be running for DynamoDB Local):
   - `mvn test -Dtest=DynamoDbMetadataStoreTest` — persistent CRUD+list + watch.
   - `mvn test -Dtest=DynamoDbPartitioningMetadataStoreTest` — partitioned CRUD,
     the ephemeral-liveness/TTL case, and partitioned watch.
   - `mvn test -Dtest=DynamoDbBridgeWiringTest` — config-driven selection through
     the `DatasetMetadataStore` / `SearchMetadataStore` bridge delegate.
4. Format: `mvn fmt:format`.
