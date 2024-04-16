# Cluster recommendations

Recommendations for cluster configuration including hardware configurations for CPU, RAM, and disk.

## General
* ZK session timeout: 60s
* ZK connect timeout: 15s
* ZK sleep retries: 5s
* JVM flags:
    * `-Dcom.linecorp.armeria.transportType=io_uring `

## Index
|---|---|
|||
|Instance type|`r5d.24xlarge`|
|CPU cores|2-5|
|Memory|32GB|
|JVM memory|6GB|
|Localdisk|90Gi|

### Other configs
```yaml
maxBytesPerChunk: 15000000000 # 15GB
```
Scaled to 4MB/s per indexer - 40MB/s cluster would be 10 nodes

## Recovery
|---|---|
|||
|Instance type|`r5d.24xlarge`|
|CPU cores|2-5|
|Memory|24GB|
|JVM memory|20GB|
|Localdisk|100Gi|

Auto scaled on CPU > 60%, min 2 nodes

## Manager
|---|---|
|||
|Instance type|`m5.24xlarge`|
|CPU cores|0.5-2|
|Memory|12GB|
|JVM memory|8GB|

1 instance per cluster

## Query
|---|---|
|||
|Instance type|`m5.24xlarge`|
|CPU cores|1-4|
|Memory|32GB|
|JVM memory|28GB|

```yaml
requestTimeout: 60s
```
Scaled to 3-10 nodes, depending on query load

## Cache
|---|---|
|||
|Instance type|`i3en.24xlarge`|
|CPU cores|5-8|
|Memory|42GB|
|JVM memory|30GB|
|Localdisk|3300Gi|

```yaml
cacheSlotsPerInstance: 200
requestTimeout: 55s
```

Auto scaled with HPA using the hpa_cache_demand_factor metric targeting 1.0

## Preprocessor
|---|---|
|||
|Instance type|`m5.24xlarge`|
|CPU cores|2-4|
|Memory|36GB|
|JVM memory|28GB|

Using bulk ingest targeting around 25MB-35MB/s per instance