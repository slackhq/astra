# Indexer

The indexer nodes are responsible for consuming messages from their assigned Kafka partition, indexing this data
into a Lucene index, and providing query capabilities until this chunk is successfully uploaded to S3.

## System design

Write path: The indexer component consists of several indexers. Each indexer performs the following tasks on the write
path:

* Each indexer reads the logs from a specific Kafka partition.
* The logs read from Kafka are indexed into a Lucene index on a local disk in an active chunk. At this time the index is open for both reads and writes.
* Once a chunk reaches a specific size or after a specific offset (configurable) the current chunk is closed for writes and the chunk becomes read-only. Once the new chunk is created it becomes the active chunk that accepts the writes.
* The data in the read-only chunk will be uploaded to S3 and this segment is registered in the cluster manager containing chunk metadata such as path, start time, and end time.
* The indexer periodically runs a cleaner task that deletes the chunks older than N hours from the indexer.

Read path: The indexer exposes an HTTP endpoint that is used to query the indexed data. For every query, the indexer
searches the relevant chunks for the time range, groups all the data together, and returns a response.

### Indexer lifecycle
An indexer is responsible for reliably indexing the data and uploading the result into S3. It is also responsible for
serving reads for recent data.

### Picking an index offset

Every indexer has an id that is assigned a partition. When an indexer starts indexing the data, it looks at the most
recent snapshot published by the current indexer. If that offset-current head < N, it starts indexing at that that
offset. Otherwise, the indexer would create a recovery task for that partition from the last indexed offset to current
head and start indexing the current partition from the current head.

### Indexer Snapshot and search metadata management

The indexer manages the data it owns as a list of chunks. Each chunk contains the data for a certain period of time or
size. The indexer has one active chunk that gets the writes and the remaining chunks are read only chunks. Once a chunk
is full (met the size of time requirement for a chunk), the indexer uploads the data to S3 and publishes a snapshot
metadata, and after a configurable amount of time deletes the read-only snapshot from indexer since it would be served
by the cache instance. In the future, if needed we can add additional checks to ensure that the snapshot is being served by
another node before it’s deleted from the indexer.

The process described above describes the data write path but not the read path.

Every time an indexer creates a new chunk it also creates an active snapshot metadata. The active snapshot metadata
contains the creation time of the current chunk, an empty blob store path and an infinite end time. In addition, the
indexer also publishes a search metadata node for the indexer so this indexer can be found by the query nodes for the
active snapshot. When a chunk is no longer active and the chunk is uploaded to S3, the indexer creates a search
metadata node with the updated snapshot metadata and deletes the active snapshot and the active search metadata node.
The cache assignment protocol assigns the snapshot to the cache node.

## Basic operation
### Build & deploy
Indexers can be restarted at any time, but should not be unnecessarily. This is due to the fact that a restart causes
the pending indexed data to be discarded, and re-assigned for indexing on a recovery node. This will result in temporary
unavailability of these logs until the recovery nodes have indexed and uploaded the data to S3, and it has been
downloaded by a cache node.

Indexers are currently deployed as a stateful set, so that they have unique allocated numeric IDs. These IDs start from
0, and correlate to the indexer's assigned partition number.

### Capacity
Each indexer is single threaded, and generally can index between 2MB/s and 3MB/ depending on specifics of the shape of
the incoming data. Adding additional indexers requires that the kafka topic have equal to or greater partitions to the
indexer count, and that the assigned partitions/throughput in the manager UI are updated accordingly.
