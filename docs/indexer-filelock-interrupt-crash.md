# Indexer `FileLock invalidated` crash

## Symptom

Indexer pods die via `System.exit(1)` (hundreds of VM halts per week) with:

```
org.apache.lucene.store.AlreadyClosedException: FileLock invalidated by an external force:
  NativeFSLock(path=/astra_data/indices/<chunk-id>/write.lock, impl=...[... invalid], ...)
```

The error surfaces during a Lucene write (`commit`, `forceMerge`, or chunk `close()`), then
propagates up to `AstraIndexer.run()`, which treats any exception as fatal and halts the VM.

## Root cause (mechanism)

A **leaked Java thread interrupt flag** destroys the Lucene `write.lock`.

The chain:

1. Lucene validates the `write.lock` on many operations via
   `NativeFSLock.ensureValid()`, which calls `channel.size()` on the lock's
   `FileChannel`.
2. `FileChannel` is an `InterruptibleChannel`. If the calling thread's **interrupt flag is
   already set** when it enters a blocking channel op, the JDK's NIO machinery **closes the
   channel** (`ClosedByInterruptException`).
3. Once that channel is closed, the underlying `FileLock` is invalid **forever**.
   `FileLock.isValid()` returns false, so every later `ensureValid()` throws
   `FileLock invalidated by an external force`.
4. The write.lock is shared by the whole index (chunk), so the entire chunk is now broken.
   The next write fails fatally → VM halt.

### The flag is *dormant*

The interrupt does not have to happen during the failing operation. A flag set earlier just
**sits set on a reused pool thread** until the next op that touches a channel:

- Ops that block/wait (`forceMerge` → internal wait, `awaitTermination`, the writer's event
  queue) observe the flag, throw `InterruptedException`, **consume** the flag, and the lock
  *survives*.
- A no-op `commit()` with nothing to flush never touches the lock's channel — flag stays
  dormant, lock alive.
- The **first op that actually writes to a channel** with the flag still set (a flush, a file
  delete, or `ensureValid` → `channel.size()`) is the one that closes the channel and kills
  the lock.

This is why the production stack traces throw at the `!lock.isValid()` check rather than at
`channel.size()`: the kill happened on an *earlier* operation, and the observed failure merely
inherited an already-dead channel.

### This is a known, unfixed Lucene behavior

This is not an Astra bug in Lucene's sense — it is documented upstream and Lucene declined to
defend against it:

- **LUCENE-8262** — `NativeFSLockFactory`'s `FileChannel` closes on interrupt, permanently
  invalidating the lock. Resolved **Won't Fix**.
- **apache/lucene#9309** — same mechanism, same symptom. Resolved **Won't Fix**.

Both issues proposed switching to `AsynchronousFileChannel` (which is not an
`InterruptibleChannel`); both were rejected. Lucene's guidance is explicit: **do not interrupt
threads that perform Lucene I/O.** The responsibility to avoid the interrupt sits with the
caller — us.

## The problem we still have to solve: *what sets the flag?*

Knowing the mechanism is not enough to fix it, because we do not yet know **what interrupts the
Lucene-I/O threads in steady state.** A production crash was captured on the **scheduled commit
thread** (a private, per-store single-thread executor) during a normal commit — not during
shutdown. Nothing outside `LuceneIndexStoreImpl` holds a reference to that thread, which
narrows the origin to:

1. **Self-restoration inside Lucene** — a prior op on the same thread caught an
   `InterruptedException` and restored the flag (`Thread.currentThread().interrupt()`), which
   our `catch` then logged and swallowed *without clearing it*, leaving it on the pooled thread
   for the next commit. (Consistent with the captured steady-state crash.)
2. **Executor shutdown escalation** — `ExecutorService.close()` / `shutdownNow()` interrupting
   the worker during shutdown. (Does not match a steady-state commit crash.)
3. External `.interrupt()` on the thread — effectively impossible; no code holds the reference.

The JVM gives us no callback for "a thread was interrupted," so in the general case we cannot
see *who* set the flag.

## Current change: diagnostics to identify the trigger

This branch does **not** add a barrier or otherwise alter behavior. It adds an
`InterruptLoggingThreadFactory` that produces threads whose `interrupt()` override logs the
**caller's stack** (via a never-thrown `Throwable`) synchronously, then delegates to
`super.interrupt()`. This captures the one piece we cannot otherwise observe — the code that
interrupts a Lucene-I/O thread — for both the self-restoration and shutdown-escalation cases.

Applied to the three reused Lucene-I/O executors:

- `LuceneIndexStoreImpl` scheduled **commit** (`lucene-commit-%d`) — the thread of the captured
  prod crash.
- `LuceneIndexStoreImpl` scheduled **refresh** (`lucene-refresh-%d`).
- `IndexingChunkManager` **rollover** pool (`lucene-rollover-%d`), which also runs the
  stale-chunk `close()` via a `directExecutor` callback.

The factory is diagnostic-only: it logs and delegates, it does not clear or suppress the
interrupt. Volume is low (these threads are rarely interrupted), so it is safe to run in prod.

### What to look for in prod

A WARN of the form:

```
Thread 'lucene-commit-0' performing Lucene I/O was interrupted; capturing caller stack ...
```

The attached stack identifies the trigger:

- If it points into Lucene internals restoring the flag → theory (1), self-restoration.
- If it points into an executor `close()` / `shutdownNow()` path → theory (2), shutdown
  escalation.

Cross-reference the WARN timestamp with rollover / shutdown events on the same pod.

## Deferred follow-up: the fix

Once the trigger is identified, the fix should remove it **at the source** (e.g. stop the
offending code from interrupting a Lucene-I/O thread, or sever the `directExecutor` chain that
lets an S3-upload interrupt reach a `close()`). A blanket `Thread.interrupted()` barrier at each
worker entry was considered as a catch-all backstop but is deliberately **not** included here:
we want the trigger data first, so the eventual fix is targeted rather than defensive.

### A separate, confirmed trigger in the S3 upload path

`BlobStore.awaitUploads` catches `InterruptedException`, **restores** the flag
(`Thread.currentThread().interrupt()`), and throws a `RuntimeException`. That exception is
caught and swallowed by `ReadWriteChunk.snapshotToS3` (`catch (Exception) { return false; }`),
orphaning the restored flag on the **rollover thread** — which then runs the stale-chunk
`close()` inline via `directExecutor`. This is a clear leak on the rollover path, independent of
the still-unknown commit-thread trigger, and is a candidate for the source-level fix above.
