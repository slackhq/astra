# Webapp-Errors Cache Node OOM Investigation

## Status: P0 Fixes Implemented (pending deploy)
## Date: 2026-02-20

## Problem
Cache nodes on the `webapp-errors` cluster (logging-prod-iad) are regularly OOMing. This is a small cluster (~3 TB) but heavily queried. The OOMs are NOT happening on much larger clusters (~500 TB).

## Root Cause: Three Compounding Issues

### Issue 1: No Circuit Breaker on Aggregation Memory (THE SMOKING GUN)

**File: `AstraBigArrays.java:24`**
```java
bigArray = new BigArrays(pageCacheRecycler, new NoneCircuitBreakerService(), "NoneCircuitBreaker");
```

**File: `OpenSearchAdapter.java:323`**
```java
new NoneCircuitBreakerService()  // Also used for IndexFieldDataService
```

In real OpenSearch, the circuit breaker throws `CircuitBreakingException` when memory gets too high, gracefully rejecting the query. Astra has **explicitly disabled this** with `NoneCircuitBreakerService`. This means aggregations can allocate memory without limit until the JVM OOMs.

The OOM stack trace confirms this:
```
GlobalOrdinalsStringTermsAggregator$RemapGlobalOrds.collectGlobalOrd
  -> CardinalityAggregator$DirectCollector.collect
    -> HyperLogLogPlusPlus$HyperLogLog.ensureCapacity
      -> BigByteArray.resize
        -> Arrays.copyOf  <-- OOM here (unbounded allocation)
```

A terms aggregation on a high-cardinality field creates N buckets, each with a HyperLogLogPlusPlus estimator for the nested cardinality sub-aggregation. With no circuit breaker, this grows until OOM.

### Issue 2: No Limit on Aggregation Bucket Count

OpenSearch has `search.max_buckets` (default 65,535) to cap how many buckets a terms/histogram aggregation can create. Astra has **no such limit**. A terms aggregation on a field with millions of unique values will try to create millions of buckets.

### Issue 3: No Request-Level Concurrency Limiting on Cache Nodes

- The `astra.concurrent.query=20` semaphore only limits chunk-level parallelism (how many chunks are searched simultaneously)
- There is NO limit on how many gRPC requests can be processed concurrently
- Multiple expensive aggregation queries arriving simultaneously each consume heap independently
- The gRPC/Armeria server has no configured `maxConcurrentRequests` or backpressure

## Why Webapp-Errors Specifically?

1. **Small cluster, high query load**: Fewer cache nodes means each node sees more queries per second
2. **Terms + cardinality aggregations**: The specific Grafana dashboards/alerts querying this cluster use terms aggregations with nested cardinality sub-aggregations on high-cardinality fields
3. **Larger clusters dilute the load**: On a 500 TB cluster with many more cache nodes, the same query spreads across more nodes, and each node processes fewer chunks per query
4. **Memory already near limit**: Steady-state usage is 37-46 GiB out of 45 GiB limit, leaving almost zero headroom for query spikes
5. **Simultaneous crashes**: Multiple pods crashed at the exact same time (18:47:36), confirming a single query (likely from a Grafana alert) fans out to multiple cache nodes and OOMs them simultaneously

## Memory Configuration
- Pod memory limit: 45 GiB (request = limit, no bursting)
- JVM heap: 32 GB (-Xms32g -Xmx32g)
- Off-heap headroom: ~13 GiB (for mmap'd Lucene segments, native memory, etc.)
- Steady-state memory: 37-46 GiB across pods
- GC: ZGC (Generational)

## Concurrency Configuration
- `astra.concurrent.query=20` (semaphore for chunk-level queries, NOT incoming requests)
- No gRPC-level concurrency limiting
- Each cache node holds 27-55 chunks
- Query node fans out to ONE cache node per snapshot (not broadcast to all)

## Code Path Analysis

### 1. ChunkManagerBase.java (query entry on cache node)
- Finds chunks matching time range, forks one task per chunk
- Semaphore (20) limits concurrent chunk searches
- Catches OutOfMemoryError → calls RuntimeHalterImpl.handleFatal() → process exits

### 2. LogIndexSearcherImpl.java:138 (per-chunk Lucene search)
- When aggregations present: `totalHitsThreshold = Integer.MAX_VALUE`
- Forces ScoreMode.COMPLETE - must iterate ALL matching documents (no early exit)
- Each hit deserialized from JSON (buildLogMessage), creating Map objects

### 3. OpenSearchAdapter.java:213-259 (aggregation execution)
- Creates Aggregator per chunk using AstraBigArrays (no circuit breaker)
- Terms + cardinality = HyperLogLogPlusPlus per bucket, unbounded growth

### 4. SearchResultAggregatorImpl.java:173-178 (local result merging)
- Flattens ALL hits from ALL chunks into one list, sorts, then limits
- TODO on line 169: "Instead of sorting all hits using a bounded priority queue of size k is more efficient"
- With 50 chunks × howMany hits each = massive in-memory sort

### 5. No `howMany` validation on ES API path
- OpenSearchRequest.java: `body.get("size").asInt()` - accepts any integer
- A request with `size: 1000000` would try to collect and deserialize 1M docs per chunk

## Recommended Fixes (Prioritized)

### P0 - Immediate (prevents OOM crashes)

1. **Replace `NoneCircuitBreakerService` with a real circuit breaker**
   - Files: `AstraBigArrays.java`, `OpenSearchAdapter.java`
   - Use OpenSearch's `HierarchyCircuitBreakerService` with a heap percentage limit
   - When breaker trips, query fails gracefully with an error instead of OOMing the process
   - This is the single highest-impact fix

2. **Add `max_buckets` limit for aggregations**
   - Cap terms/histogram aggregations at a reasonable bucket count (e.g., 65,535 like OS default)
   - Prevents runaway bucket creation on high-cardinality fields

### P1 - Short Term (reduces OOM likelihood)

3. **Add gRPC-level request concurrency limiting on cache nodes**
   - Configure Armeria's `maxConcurrentRequests` or add a request-level semaphore
   - Prevents pileup of expensive queries

4. **Validate `howMany` / `size` parameter**
   - Cap at a reasonable maximum (e.g., 10,000) in the OpenSearch API handler
   - Prevents accidental or malicious large result set requests

5. **Reduce `astra.concurrent.query` from 20 to ~8-10**
   - Quick config change, reduces concurrent memory pressure
   - Trade-off: slightly higher query latency

### P2 - Medium Term (improves efficiency)

6. **Implement bounded priority queue in SearchResultAggregatorImpl** (the existing TODO)
   - Replace `.flatMap().sorted().limit()` with a min-heap of size howMany
   - O(N log K) instead of O(N log N) and much lower peak memory

7. **Increase pod memory limit** (band-aid)
   - More headroom helps but doesn't fix the fundamental issue of unbounded allocation

## Files Referenced
- `astra/src/main/java/com/slack/astra/logstore/opensearch/AstraBigArrays.java` - Circuit breaker config
- `astra/src/main/java/com/slack/astra/logstore/opensearch/OpenSearchAdapter.java` - Second NoneCircuitBreakerService
- `astra/src/main/java/com/slack/astra/chunkManager/ChunkManagerBase.java` - Query execution + OOM handling
- `astra/src/main/java/com/slack/astra/logstore/search/LogIndexSearcherImpl.java` - Per-chunk Lucene search
- `astra/src/main/java/com/slack/astra/logstore/search/SearchResultAggregatorImpl.java` - Result merging
- `astra/src/main/java/com/slack/astra/logstore/search/AstraLocalQueryService.java` - Cache node query entry point
- `astra/src/main/java/com/slack/astra/server/ArmeriaService.java` - gRPC server (no concurrency limits)
- `kaldb-bedrock/kaldb-cache/gondola.yaml` - Deployment configuration
