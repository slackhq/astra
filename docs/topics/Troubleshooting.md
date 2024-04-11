<show-structure for="chapter,procedure" depth="2"/>

# Troubleshooting

Identify common operational issues, along with steps to remediate and prevent reoccurrence.  

## General

### Query results & performance

As part of each response from the primary query path (`_msearch`), additional metadata is provided in the response
body that may assist in troubleshooting. This additional data includes shard performance under the `responses._shards`
path, and the trace ID for the originating request under the `_debug.traceId` path.

```json
{
  "took": 0,
  "responses": [
    {
      "took": 4,
      "timed_out": false,
      "_shards": {
        "total": 6,
        "failed": 0
      },
      ...
    }
  ],
  "_debug": {
    "traceId": "5e8ea036498dd4d4"
  }
}
```

## Indexer

### Messages or bytes dropped

In the event messages are incoming that exceed the configured service's downstream rate ,messages will be dropped
randomly as the byte rate exceeds the limit. The service is configured with a small "warm-up" period to allow it to
catch up after a deploy, but if the configured limit is close to the current incoming message volume messages may be
briefly dropped during a deploy.

### No/low indexer rollovers

This indicates that indexers are not creating chunks at a frequency that is expected. Rollovers are determined by either
the index size or amount of messages received. If these are low or not present this indicates that the indexers are
either not receiving messages from the preprocessor, or that uploads are failing to post to S3.

## Cache
### Native memory allocation (mmap) failed

This happens when a sufficiently large amount of cache slots are configured on a cache node. By default the linux kernel is configured with a limit of `65536` for the `vm.max_map_count`. Each cache slot on Astra can consume several hundred mmap entries, which will eventually cause the JVM to crash with an error similar to the following:

```yaml
# There is insufficient memory for the Java Runtime Environment to continue.
# Native memory allocation (mmap) failed to map 65536 bytes for committing reserved memory.
```

Increasing the kernel limit like the following will prevent this for most use cases:
```bash
sysctl -w vm.max_map_count=262144
```
To observe the current mmap limit:
```bash
sysctl vm.max_map_count
```
To monitor the currently consumed process mmap count, substitute the PID of the Astra JVM into `[PROVIDE_ASTRA_JAVA_PID_HERE]`. This will print the current consumed value every 5 seconds, along with the current timestamp.
```bash
while true; do cat /proc/[PROVIDE_ASTRA_JAVA_PID_HERE]/maps | wc -l; date; sleep 5; done
```

### Chunk assignment or evictions failed
This indicates that the cache nodes are successfully receiving assignments, but are failing to download or evict and
register with the cluster manager. This can be potentially caused by exceeding the configured timeout for an S3
download, if the cache node is having difficulty communicating with Zookeeper, or there are issues with disk capacity.

### High assignment duration
This normally indicates that chunks are taking a long time download from S3. This could be due to S3 performance
issues, but would be more likely due to network saturation of the host, or unexpectedly large chunks.

## Preprocessor

### Incoming vs outgoing bytes difference on Preprocessor
Small differences in the bytes between incoming and outgoing are not unexpected. This is due to the fact the
preprocessors are not directly forwarding messages, but alter and may completely drop the source message.

## Recovery

### Large amount of pending recovery tasks
Recovery tasks should generally not exceed the count of indexer nodes. In normal operation every deploy would be
expected to create on-average one task per indexer, and this should recover quickly (target being under an hour). If
multiple deploys are performed, recovery nodes are failing to process tasks, or indexers are in a reboot loop this can
cause recovery tasks to build up in excess of the count of indexers.

### Recovery tasks taking a long time
If recovery tasks are taking an abnormally long time (~> 30 min) this is indicative of a few potential issues. The
uploads to S3 may be slow, the recovery tasks may have been created with too large, or disk I/O may be impacted.

