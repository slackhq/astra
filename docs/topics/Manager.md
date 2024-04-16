# Manager

The manager node is responsible for coordination over the entire system. This node handles creation of replicas,
assignments for cache nodes and recovery indexers, and is responsible for detecting system anomalies.

## System design
The cluster manager is responsible for the following tasks:

* Ensure smooth data hand-off between indexer → S3 → cache nodes.
* Maintain a registry of all the nodes in the cluster.
* Manage snapshots and chunk metadata.
* Enforce data retention policies for data
* Manage indexer and recovery nodes.

### Metadata types
|Metadata type	| Notes	                                                                                                                                                                                                                                                                                                                             |Node type	|Updatable	|
|---	|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---	|---	|
|Service(DataSet)	| Contains the name of the service and a list of partitions associated with it.	                                                                                                                                                                                                                                                     |Persistent	|Yes	|
|Snapshot	| Contains the data about the snapshot like service name, name, blob store path, start time, end time of the data in it.	                                                                                                                                                                                                            |Persistent	|No	|
|Replica	| Replica metadata is assigned to a cache node. It contains a pointer to the snapshot being served and the status of the snapshot.	                                                                                                                                                                                                  |Persistent	|No	|
|CacheNode	| Contains the name of the cache node and it's status. A cache node contains the following states depicting it's life cycle: Unknown, Free (for assignment), Assigned (assigned a replica), Loading (loading data from S3), Live (available for query), Evict (evict the assigned replica), Evicting (evicting the data from cache). |Ephemeral	|Yes	|
|Partition	| Contains partition information. Each partition has a name, partitionId and a kafka topic and partition id.	                                                                                                                                                                                                                        |Persistent	|Yes	|
|RecoveryTask	| A metadata node indicating a recovery task. The recovery task contains the following info: kafka partition id, startOffset and end offset.	                                                                                                                                                                                        |Persistent	|No	|
|RecoveryNode	| Recovery node metadata will a name and a status. The status can be one of the following: Unknown, Free, Assigned, Recovering, Free.	                                                                                                                                                                                               |Ephemeral	|No	|
|Search	| SearchMetadata consists of a snapshot name and a url serving the snapshot.	                                                                                                                                                                                                                                                        |Ephemeral	|No	|

### Cluster manager components
The following components are in cluster manager to orchestrate these operations:

| Component	               |Purpose	|Runs when	|
|--------------------------|---	|---	|
| Replica creator	         |Creates replicas out of snapshots for assignment.	|Change in snapshots metadata.	|
| CacheAssigner	           |Assigns replicas to cache nodes using cache assignment protocol. |Change in replica metadata or cache node metadata.	|
| CacheEvictor	            |Evicts replicas from cache.	|Runs periodically.	|
| RecoveryTaskAssigner	    |Assigns recovery tasks to recovery nodes.	|Change in recovery tasks.	|
| Snapshot manager	        |Manages snapshot metadata and deletes older snapshot data from S3.	|Runs periodically.	|
| Restore manager(TBD)	    |Restores older indices by creating replicas.	|Runs on demand when an external request is placed.	|
| Cluster Monitor(TBD)	    |Monitors the state of metadata nodes. Identify cache nodes stuck in a specific state, recovery nodes stuck in a specific state, invalid snapshot metadata etc..	|Runs periodically.	|
| PartitionAllocator(TBD)	 |Allocates partitions to indexers and ensures multi-tenancy.	|Runs on demand when a new services are added to the cluster.	|

### Cache assignment protocol

#### Replica creation

**Why replicas:** Instead of assigning the snapshots directly to nodes, we create a replica for each snapshot and assign them to the cache layer.  This level of indirection helps us to setup 0 or more replicas of a snapshot. Further, it also helps us add different kind of snapshot assignment like restore snapshots etc..

The replica creation manager listens for changes to snapshot metadata. When ever there is a change in snapshot metadata, the replica creation manager runs and creates replicas for the snapshots with missing replicas per the replica creation policy.

Since multiple snapshots can be published on the hour by several indexer nodes, running replica creation logic for each snapshot modification may be in-efficient. For more efficient operation, the replica creation task waits a small amount of time (a few seconds) after a notification is fired before it performs an assignment.

#### Replica assignment to cache nodes.

* Replica assignment operation runs on a cache metadata change or a replica metadata change.
    * Cache metadata change since the number of cache nodes has changed.
    * Number of replica metadata nodes has changed.
    * *Note: our cache listening strategy may need to change since we also fire an alert on cache node data change.*
* On such a notification, we first identify all the unassigned replicas and a list of free cache nodes.
* Assign the unassigned replicas to the ***CacheNode*** whose status is *Free*. If there are not enough cache nodes to assign to replicas, prioritize assigning the freshest replicas first since those nodes are more likely to be searched. Also, fire an alert if there is insufficient cache pool capacity.
* To assign a replica to a cache node, we update the CacheNode’s status field to *Assigned* status with the name of the replica assigned to it.
* Every CacheNode is also watching changes to the node. So, once a CacheNode see’s that a replica is assigned, it updates it’s status to *Loading* and starts downloading the snapshot from S3.
* Once the cache node downloaded the data from S3, initialized the Chunk and the data is ready for serving, the CacheNode sets it’s status to *Live*. Once the cache node’s status is *Live*, that data is available for query.
* Once a cache node is Live it also publishes a search metadata node allowing the cache node to be searched.
* If the cache node dies at point in this lifecycle, since CacheNode is ephemeral, a new cache assignment operation will be started all over again by the cluster manager for the replica assigned to this cache node.


**Monitoring**

* Monitor for insufficient cache pool.
* Cache nodes stuck in assignment issues. We need to monitor those nodes depending on the timestamp of the last updated cache node status and take a corrective action.
* Monitor Zombie replicas: A cache node is assigned a non-existent replica. Identify those situations and mark that cache node as free.

#### Cache eviction

* Cache eviction process is responsible for evicting the replicas from the cache when they have aged out. This process runs periodically on the cluster manager.
* During each run, the cache eviction process identifies the expired replicas. An expired replica is one which is created prior to N hours before current time.
* Once the expired replicas are identified, we then identify expired cache nodes i.e, the cache nodes still serving the expired replicas.
* Once the expired cache nodes are identified, set their status to evict so the cache nodes can start the eviction process.
* Once the cache node notices that its status is set to Evict, it set it’s state to Evicting and start the process of evicting the replica node. First it will delete the search metadata node so it doesn’t get any further search requests. Then it deletes any local data and metadata for the snapshot in the cache node. Once the eviction is complete, the cache node sets its status as free.
* The cluster manager also runs a replica cleaner step as part of every run to identify replicas that are un-assigned and expired. Once such replicas are identified, they are deleted from the replica metadata store since they no longer will be assigned.

#### Cache node lifecycle.

A cache node contains the following states depicting it's life cycle: Unknown, Free (for assignment), Assigned (assigned a replica), Loading (loading data from S3), Live (available for query), Evict (evict the assigned replica), Evicting (evicting the data from cache).

### Cache Restore protocol

#### **Why cache restore?**

A majority of the time older snapshot metadata is never accessed. So, we only cache only the last N days of data proactively on the cache tier. Data older than N days remains on S3. To search data older than N days we have a the following options:

1. On demand:
    1. Search older data by spinning off an AWS lambda task.
    2. Allocate additional capacity on the cache tier on demand, load the cache nodes there and serve the queries.
2. Pre-allocated capacity:
    1. Pre-allocate capacity on cache node to search older data.
    2. Only load older cache data on demand into a small reserved pool of cache nodes.

We should explore the on-demand proposals more in a separate design doc. This document explores the pre-allocated capacity bits since they are easier to build in the current design. The simplest option is to pre-allocate the capacity(option 2a) in the cache nodes for older data. However, since most queries only search for the last N days of data, pre-allocating capacity for all of older data would be wasteful. Since most of the services won’t be searching for older data and they are less likely to do it at the same time, a better policy(option 2b) may be one where we reserve some capacity in cache nodes for older data. Whenever a service requests a search for older data, we can load this data in this reserve pool for a short amount of time. In this doc, we will explore option 2b more.

#### Cache restore assignment protocol

* When ever a user searches for data older than N days, the query service requests the cluster manager to load those snapshots on demand also called restore those snapshots.
* Once such a request is received, the cluster manager queues a task to restore those snapshots.
* The restore assigner thread, takes all the request and creates replicas for those snapshots. These *restore replicas*, differ from regular replicas in that the restore flag is set to true on these replicas to indicate that these are restored replicas.
* The replica creation kicks off the replica assignment process in the cluster manager. The replica assignment process is identical to the replica assignment process described earlier. However, instead of allocating capacity in the free pool of cache nodes, we will allocate these replicas in the pool of cache nodes reserved for restoration. To keep it simple, the cluster manager reserves a certain percentage of cache node slots for restore.
* Once a restore replica is assigned to a cache node, the cache nodes goes through its life cycle actions including eviction.

#### Restored replica eviction protocol

* The restored replica eviction protocol is identical to the cache eviction protocol with one difference. While the cache nodes are evicted when they are no longer fresh (older than N days), the restored replicas are evicted after N hours since they are loaded. Other than the eviction time, the remaining processes are identical.

### Snapshot cleaner protocol.

* Every indexer publishes a snapshot with the snapshot the creation time of the snapshot and a URL of the snapshot metadata.
* The cluster manager periodically runs a background job that identifies the snapshots that are out of the retention period (can vary by service) and cleans them up.
    * Eviction: During the cleanup process, the snapshot manager will first evict any assigned snapshots that are outside the retention period.
    * Snapshot metadata deletion: Once a snapshot is outside retention and not assigned, we delete the replicas and any associated snapshot metadata. Then we will also delete the data in the S3 URL.
* S3 reconciliation: The S3 cleaner process will periodically reconcile the snapshot metadata URLs with S3 data and report any missing data as a a metric to the user. If extra data exists on S3, it will also delete that data.
* For safe operation, the snapshot cleaner will buffer the retention period by a few more additional hours so we can be sure that those snapshots are not being searched for.

## Basic operation
### Configuration UI
The configuration UI for the manager can be found at http://localhost:8083/docs/

### Changing allocated throughput
Changing the allocated throughput can be done using the Docs Service UI.

### Build & deploy
⚠️ Only a single manager node can be deployed at one time. If multiple managers are deployed for the same cluster they
will compete with each other, overwriting data.



