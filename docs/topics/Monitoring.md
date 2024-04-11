# Monitoring

Prometheus metrics exposed by Astra that can be used for dashboards and alerts.     

### Indexer rollover alert
Not enough rollovers are occurring (chunks being created). See [No/low indexer rollovers](Troubleshooting.md#no-low-indexer-rollovers).

```
sum by(astra_cluster_name) (increase(rollovers_completed_total{pod=~"astra.*",astra_component="index",astra_cluster_name="traces"}[90m]))
```

### Indexer rollover failure alert
Rollovers are failing to successfully complete. The most likely cause for this would be a failure to upload to S3,
or potentially the inability to persist the metadata to Zookeeper.

```
sum by(astra_cluster_name) (increase(rollovers_failed_total{pod=~"astra.+traces.+",astra_component="index"}[5m]))
```

### Replica assignment capacity
This number indicates the available capacity of the cache nodes; positive is excess capacity, negative is shortage of
capacity. To resolve this add additional cache capacity per instructions in [Adding capacity](Cache.md#adding-capacity), or
reduce configured retention. Until this is resolved, results will be incomplete for queries.

```
sum by (replicaset) (sum_over_time(replica_assign_available_capacity{astra_cluster_name="traces",astra_component="manager"}[1m]) / count_over_time(replica_assign_available_capacity{astra_cluster_name="traces",astra_component="manager"}[1m]))
```

### Results visible to query
The amount of results being returned by Astra when querying for the last five minutes is below the alerting threshold.
The first step should be to identify if this is just affecting recent results, or all results. If no results are
available for query, regardless of timeframe, this indicates that the query nodes may be experiencing an issue,
or Zookeeper may be having an issue.

If only recent results are missing, this would indicate an issue with the preprocessors or indexers. One potential source may be lag or trouble connecting to Kafka, which would prevent ingestion of new data.

### Cached recovery tasks size alert
This indicates that too many pending recovery tasks exists in the queue, see
[Large amount of pending recovery tasks](Troubleshooting.md#large-amount-of-pending-recovery-tasks). Scaling the recovery node count
would be the quickest way to resolve this, followed by understanding what caused the unexpected increase in tasks.

```
sum by (astra_cluster_name)(cached_recovery_tasks_size{pod=~"astra.+traces.+"})
```