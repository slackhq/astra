# Cache

The cache nodes are responsible for downloading chunks from S3, and serving these until the chunk expiration.
Cache nodes register a configurable amount of slots, and then the manager is responsible for handling assignments of
specific chunks to these slots,

## System design
These nodes are identical to indexer nodes except that these nodes download the chunks from S3 (instead of Kafka)
and serve reads. The cache tier loads the Lucene indexes from S3 into the local disk the last day (configurable).

## Basic operation
### Build & deploy
Cache nodes should be deployed as slowly as reasonably possible. This is due to the fact that a cache node going offline
needs to have its assignments picked up and downloaded by another cache node. If too many cache nodes are being
restarted at once this can cause a temporary reduction in chunks served for queries, resulting in incomplete data.

<warning><p>The cache nodes make significant use of Linux virtual memory via <tooltip term="mmap">mmap</tooltip>. The default value is highly likely to be too low for most use cases, and <a href="Troubleshooting.md" anchor="native-memory-allocation-mmap-failed">should be increased</a>.</p></warning>

### Adding capacity
Capacity can be safely added or reduced at any time. The manager will attempt to prioritize newer chunks in the event
the configured replica retention is higher than the available capacity. This would appear in operation if you configured
capacity for 24 hours, and noticed only the latest 12 hours were available.

This capacity can be seen by using the following Prometheus query:
```
sum by (replicaset) (sum_over_time(replica_assign_available_capacity{astra_cluster_name="${cluster}",astra_component="manager"}[1m]) / count_over_time(replica_assign_available_capacity{astra_cluster_name="${cluster}",astra_component="manager"}[1m]))
```

