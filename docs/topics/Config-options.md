<show-structure for="chapter,procedure" depth="2"/>

# Config file reference
This is a reference of all available options that can be set in an Astra config file, and recommended values.

## nodeRoles
Defines the mode to operate a node in. Astra ships as a single binary to simplify deployments, and this configuration 
is used to select which operating mode to run in.

```yaml
nodeRoles: [QUERY,INDEX,CACHE,MANAGER,RECOVERY,PREPROCESSOR]
```
<warning><p>Multiple node role support is deprecated and will be removed in a future release</p></warning>
<deflist type="medium">
<def title="QUERY">
A query node provides http API for querying cluster and aggregates results from indexers and cache nodes. 
</def>
<def title="INDEX">
An indexer node reads from Kafka to create a Lucene index on disk, which is then uploaded to S3. 
</def>
<def title="CACHE">
A cache node receives <tooltip term="chunk">chunk</tooltip> assignments from the manager node, which are downloaded from S3.
</def>
<def title="MANAGER">
The manager node is responsible for cluster orchestration,
</def>
<def title="RECOVERY">
A recovery node indexes data that is skipped by an indexer, publishing <tooltip term="snapshot">snapshots</tooltip>.
</def>
<def title="PREPROCESSOR">
A preprocessor node serves a bulk ingest HTTP api, which is then transformed, rate limited, and sent to Kafka.
</def>
</deflist>


## indexerConfig

Configuration options for the indexer node.

### maxMessagesPerChunk
Maximum number of messages that are created per <tooltip term="chunk">chunk</tooltip> before closing and uploading to S3. This should be roughly
equivalent to the `maxBytesPerChunk`, such that a rollover is triggered at roughly the same time regardless of 
messages or bytes.

```yaml
indexerConfig:
  maxMessagesPerChunk: 100000
```

### maxBytesPerChunk
Maximum bytes that are created per <tooltip term="chunk">chunk</tooltip> before closing and uploading to S3. This should be roughly
equivalent to the `maxMessagesPerChunk`, such that a rollover is triggered at roughly the same time regardless of
messages or bytes.

```yaml
indexerConfig:
  maxBytesPerChunk: 1000000
```

### maxTimePerChunkSeconds
Maximum time that a <tooltip term="chunk">chunk</tooltip> can be open before closing and uploading to S3. Defaults to 90 minutes.
This configuration is useful for ensuring that chunks are uploaded to S3 within a set time frame,
during non-peak hours when we don't hit maxMessagesPerChunk or maxBytesPerChunk for several hours

```yaml
indexerConfig:
  maxTimePerChunkSeconds: 5400
```

### luceneConfig

```yaml
indexerConfig:
  luceneConfig:
    commitDurationSecs: 30
    refreshDurationSecs: 10
    enableFullTextSearch: false
```

<deflist>
<def title="commitDurationSecs">
How often Lucene commits to disk. This value will impact the accuracy of calculating the 
<tooltip term="chunk">chunk</tooltip> size on disk for rollovers, so should be set conservatively to keep 
chunk sizes consistent. 
</def>
<def title="refreshDurationSecs">
How often Lucene refreshes the index, or makes results visible to search. 
</def>
<def title="enableFullTextSearch">

Indexes the contents of each message to the `_all` field, which is set as the default query field if not specified by 
the user. Enables queries such as `value` instead of specifying the field name explicitly, `field:value`.
</def>
</deflist>

### staleDurationSecs

```yaml
indexerConfig:
  staleDurationSecs: 7200
```

How long a stale <tooltip term="chunk">chunk</tooltip>, or a chunk no longer being written to, can remain on an indexer 
before being deleted. If the [indexerConfig.maxChunksOnDisk](Config-options.md#maxchunksondisk) limit is reached prior to this value the chunk will
be removed.

### dataDirectory {id=indexer-data-directory}

```yaml
indexerConfig:
  dataDirectory: /mnt/localdisk
```

<snippet id="data-directory-desc">
Path of data directory to use. Generally recommended to be instance storage backed by NVMe disks, or a memory mapped 
storage like [tmpfs](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir) for best performance. 
</snippet>

### maxOffsetDelayMessages
```yaml
indexerConfig:
  maxOffsetDelayMessages: 300000
```

Maximum amount of messages that the indexer can lag behind on startup before creating an async recovery tasks. If the 
current message lag exceeds this the indexer will immediately start indexing at the current time, and create a task to 
be indexed by a recover node from the last persisted offset to where the indexer started from.

### defaultQueryTimeoutMs {id=indexer-default-query-timeout-ms}

```yaml
indexerConfig:
  defaultQueryTimeoutMs: 6500
```

<snippet id="default-query-timeout-desc">
Timeout for searching an individual <tooltip term="chunk">chunk</tooltip>. Should be set to some value below the 
`serverConfig.requestTimeoutMs` to ensure that post-processing can occur before reaching the overall request timeout.
</snippet>

### readFromLocationOnStart

```yaml

indexerConfig:
  readFromLocationOnStart: LATEST
```

Defines where to read from Kafka when initializing a new cluster.

<deflist>
<def title="EARLIEST">
Use the oldest Kafka offset when initializing cluster to include all messages currently on Kafka.
</def>
<def title="LATEST">
Use the latest Kafka offset when initializing cluster, will start indexing new messages from the cluster initialization 
time onwards. See <a href="Config-options.md#createrecoverytasksonstart">indexerConfig.createRecoveryTasksOnStart</a> for 
an additional config parameter related to using <code>LATEST</code>.
</def>
</deflist>

<tip>This config is only used when initializing a new cluster, when no existing offsets are found in Zookeeper.</tip>

### createRecoveryTasksOnStart

```yaml

indexerConfig:
  createRecoveryTasksOnStart: true
```

Defines if recovery tasks should be created when initializing a new cluster.

<note>This only applies when <a href="Config-options.md#readfromlocationonstart">indexerConfig.readFromLocationOnStart</a> is set to <code>LATEST</code>.</note>
<tip>This config is only used when initializing a new cluster, when no existing offsets are found in Zookeeper.</tip>

### maxChunksOnDisk

```yaml
indexerConfig:
  maxChunksOnDisk: 3
```
How many stale <tooltip term="chunk">chunks</tooltip>, or chunks no longer being written to, can remain on an indexer 
before being deleted. If the [indexerConfig.staleDurationSecs](Config-options.md#staledurationsecs) limit is reached prior to this value the chunk 
will be removed.

### serverConfig {id=indexer-server-config}
```yaml
indexerConfig:
  serverConfig:
    serverPort: 8081
    serverAddress: 10.0.100.1
    requestTimeoutMs: 7000
```
<snippet id="server-config">
<deflist type="medium">
<def title="serverPort">
Port used for application HTTP traffic.
</def>
<def title="serverAddress">
Address at which this instance is accessible by other Astra components. Used for inter-node communication and is 
registered to Zookeeper.
</def>
<def title="requestTimeoutMs">
Request timeout for all HTTP traffic after which the request is cancelled. 
</def>
</deflist>
</snippet>

### kafkaConfig {id=kafka-indexer}

```yaml
indexerConfig:
  kafkaConfig:
    kafkaTopic: test-topic
    kafkaTopicPartition: 0
    kafkaBootStrapServers: localhost:9092
    kafkaClientGroup: astra-test
    enableKafkaAutoCommit: true
    kafkaAutoCommitInterval: 5000
    kafkaSessionTimeout: 30000
    additionalProps: "{isolation.level: read_committed}"
```

<snippet id="kafka-indexing-config">
<deflist>
<def title="kafkaTopic">
Kafka topic to consume messages from
</def>
<def title="kafkaBootStrapServers">
Address of the kafka server, or servers comma separated.
</def>
<def title="additionalProps">
String value of JSON encoded properties to set on Kafka consumer. Any valid Kafka property can be used.
<a href="https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html">Conflent Kafka Producer Configuration Reference</a>
</def>
</deflist>
</snippet>


## s3Config
S3 configuration options common to indexer, recovery, cache, and manager nodes.

<tip>

S3 compatible APIs from vendors other than Amazon may work, but are not guaranteed. Astra uses the 
[AWS CRT client](https://github.com/awslabs/aws-crt-java) for improved performance, but this library has less expected
compatibility with other vendors.
</tip>

```yaml
s3Config:
  s3AccessKey: access
  s3SecretKey: key
  s3Region: us-east-1
  s3EndPoint: localhost:9090
  s3Bucket: test-s3-bucket
  s3TargetThroughputGbps: 25
```

<deflist type="wide">
<def title="s3AccessKey">
AWS access key. If both access key and secret key are empty will use the AWS default credentials provider. 
</def>
<def title="s3SecretKey">
AWS secret key. If both access key and secret key are empty will use the AWS default credentials provider.
</def>
<def title="s3Region">

AWS region, ie `us-east-1`, `us-west-2`   
</def>
<def title="s3EndPoint">
S3 endpoint to use. If this setting is null or empty will not attempt to override the endpoint and will use the default
provided by the AWS client.
</def>
<def title="s3Bucket">
AWS S3 bucket name

<tip>Separate buckets per cluster is recommended for better cost tracking and improved performance.</tip>
</def>
<def title="s3TargetThroughputGbps">
Throughput target in gigabits per second. This configuration controls how many concurrent connections will be 
established in the AWS CRT client. Recommended to be set to match the maximum bandwidth of the underlying host.
</def>
</deflist>

<tip>See also <a href="System-properties-reference.md#astra-s3crtblobfs-maxnativememorylimitbytes">astra.s3CrtBlobFs.maxNativeMemoryLimitBytes</a>
system properties config.</tip>

## tracingConfig

``` yaml
tracingConfig:
  zipkinEndpoint: http://localhost:9411/api/v2/spans
  commonTags:
    clusterName: astra-local
    env: localhost
  samplingRate: 0.01
```

### zipkinEndpoint

Fully path to the Zipkin [POST spans endpoint](https://zipkin.io/zipkin-api/#/default/post_spans). Will be submitted as
a JSON array of span data.

### commonTags
Optional common tags to annotate on all submitted Zipkin traces. Can be overwritten by spans at runtime, if keys 
collide. 

<tip>Recommended common tags: <code>clusterName</code>, <code>env</code></tip>

### samplingRate
Rate at which to sample astra's traces. A value of `1.0` will send all traces, `0.01` will send 1% of traces, etc.

## queryConfig

Configuration options for the query node.

### serverConfig {id=query-server-config}

```yaml
queryConfig:
  serverConfig:
    serverPort: 8081
    serverAddress: 10.0.100.2
    requestTimeoutMs: 60000
```
<include from="Config-options.md" element-id="server-config"></include>

### defaultQueryTimeout

```yaml
queryConfig:
  defaultQueryTimeout: 55000
```

Query timeout for individual indexer and cache nodes when performing a query. This value should be set lower than the 
<a href="Config-options.md#query-server-config">queryConfig.serverConfig.requestTimeoutMs</a> and equal-to or greater-than
the <a href="Config-options.md#indexer-server-config">indexerConfig.serverConfig.requestTimeoutMs</a> and
<a href="Config-options.md#cache-server-config">cacheConfig.serverConfig.requestTimeoutMs</a>. 

<tip>When setting a timeout ensure that the 
<a href="Config-options.md#query-server-config">queryConfig.serverConfig.requestTimeoutMs</a> is at least a few
seconds higher than the <a href="Config-options.md#defaultquerytimeout">queryConfig.serverConfig.defaultQueryTimeout</a>
to allow for aggregation post-processing to occur.
</tip>

### zipkinDefaultMaxSpans

```yaml
queryConfig:
  zipkinDefaultMaxSpans: 25000
```

Max spans that the zipkin endpoint will return when the API call does not include `maxSpans`. A trace with more than 
this amount of spans will be truncated.

### managerConnectString
```yaml
queryConfig:
  managerConnectString: 10.0.100.2:8085
```

<tldr>experimental</tldr>
Host address for manager node, used for on-demand recovery requests.

## metadataStoreConfig
```yaml
metadataStoreConfig:
  zookeeperConfig:
    zkConnectString: localhost:2181
    zkPathPrefix: astra
    zkSessionTimeoutMs: 5000
    zkConnectionTimeoutMs: 500
    sleepBetweenRetriesMs: 100
```

### zookeeperConfig
<deflist>
<def title="zkConnectString">

Zookeeper connection string - list of servers to connect to or a common service discovery endpoint 
(ie, [consul endpoint](https://www.consul.io/)).
</def>
<def title="zkPathPrefix">

Common prefix to use for Astra data. Useful when using a common Zookeeper installation.

<tip>A shared Zookeeper cluster is only recommended for very small Astra clusters.</tip>
</def>
<def title="zkSessionTimeoutMs">
Zookeeper session timeout in milliseconds.
</def>
<def title="zkConnectionTimeoutMs">
Zookeeper connection timeout in milliseconds.
</def>
<def title="sleepBetweenRetriesMs">
How long to wait between retries when attempting to reconnect a Zookeeper session. Will retry up to the 
`zkSessionTimeoutMs`.
</def>
</deflist>

## cacheConfig

Configuration options for the cache node.

### slotsPerInstance

```yaml
cacheConfig:
  slotsPerInstance: 200
```

Defines how many cache slots are registered per cache node. This should be set so that the `slotsPerInstance` multiplied
by the [`indexerConfig.maxBytesPerChunk`](Config-options.md#maxbytesperchunk) is less than the total available space at
the [cacheConfig.dataDirectory](Config-options.md#cache-data-directory) path, plus a small buffer.

<tip>

The <tooltip term="chunk">chunk</tooltip> size on disk can overshoot the configured 
[`indexerConfig.maxBytesPerChunk`](Config-options.md#maxbytesperchunk), depending on how frequently lucene is set to 
flush to disk in the [`indexerConfig.luceneConfig.commitDurationSecs`](Config-options.md#luceneconfig) config.  
</tip>
 

### replicaSet
<tldr>experimental</tldr>

```yaml
cacheConfig:
  replicaSet: rep1
```

Unique identifier for this deployment of cache nodes. This setting, in combination with
[managerConfig.replicaCreationServiceConfig.replicaSets](Config-options.md#replicacreationserviceconfig), 
[managerConfig.replicaAssignmentServiceConfig.replicaSets](Config-options.md#replicaassignmentserviceconfig) and
[managerConfig.replicaRestoreServiceConfig.replicaSets](Config-options.md#replicarestoreserviceconfig) allow running 
multiple deployments of cache nodes in a high availability deployment.

### dataDirectory {id=cache-data-directory}

```yaml
cacheConfig:
  dataDirectory: /mnt/localdisk
```

<include from="Config-options.md" element-id="data-directory-desc"></include>

### defaultQueryTimeoutMs {id=cache-default-query-timeout-ms}

```yaml
cacheConfig:
  defaultQueryTimeoutMs: 50000
```

<include from="Config-options.md" element-id="default-query-timeout-desc"></include>

### serverConfig {id=cache-server-config}
```yaml
cacheConfig:
  serverConfig:
    serverPort: 8081
    serverAddress: 10.0.100.3
    requestTimeoutMs: 55000
```
<include from="Config-options.md" element-id="server-config"></include>

## managerConfig

Configuration options for the manager node.

### eventAggregationSecs

```yaml
managerConfig:
  eventAggregationSecs: 10
```

Configures how long change events are batched before triggering an event executor. This helps improve performance in 
clusters with a large amounts of change events (<tooltip term="chunk">chunk</tooltip> rollovers, pod turnover). 

### scheduleInitialDelayMins

```yaml
managerConfig:
  scheduleInitialDelayMins: 1
```

How long after manager startup before scheduled services should start executing.

### serverConfig {id=manager-server-config}
```yaml
managerConfig:
  serverConfig:
    serverPort: 8081
    serverAddress: 10.0.100.10
    requestTimeoutMs: 30000
```

<include from="Config-options.md" element-id="server-config"></include>

### replicaCreationServiceConfig

```yaml
managerConfig:
    replicaCreationServiceConfig:
      schedulePeriodMins: 15
      replicaLifespanMins: 1440
      replicaSets: [rep1]
```
Configuration options controlling <tooltip term="replica">replica</tooltip> creation after a 
<tooltip term="chunk">chunk</tooltip> is uploaded from an indexer.

<deflist type="medium">
<snippet id="schedule-period-mins">
<def title="schedulePeriodMins">
How frequently this task is scheduled to execute. If the time to complete the scheduled task exceeds the period, the 
previous invocation will be cancelled and restarted at the scheduled time.
</def>
</snippet>
<def title="replicaLifespanMins">
How long a <tooltip term="replica">replica</tooltip> associated with a <tooltip term="chunk">chunk</tooltip> will exist 
before expiring.

<note>This is the primary configuration for the cluster's searchable retention.</note>
<tip>

Changing this will apply to **future** <tooltip term="replica">replicas</tooltip>, and does not apply to previously 
created replicas.
</tip>
<snippet id="replica-sets">
<def title="replicaSets">

Array of cache <tooltip term="replica">replica</tooltip> set identifiers 
([cacheConfig.replicaSet](Config-options.md#replicaset)) for this task to operate on.
</def>
</snippet>
</def>
</deflist>

### replicaAssignmentServiceConfig

```yaml
managerConfig:
  replicaAssignmentServiceConfig:
    schedulePeriodMins: 15
    replicaSets: [rep1]
    maxConcurrentPerNode: 2
```
Configuration options controlling <tooltip term="replica">replica</tooltip> assignments to available cache nodes.

<deflist type="wide">
<include from="Config-options.md" element-id="schedule-period-mins"></include>
<include from="Config-options.md" element-id="replica-sets"></include>
<def title="maxConcurrentPerNode">
Controls how many assignment will concurrently execute. Setting this to a low value allows 
<tooltip term="replica">replicas</tooltip> to become available quicker as they do not compete for download bandwidth, 
and allows newly created replicas of higher priority to be downloaded before a long list of lower priority replicas.
</def>
</deflist>

### replicaEvictionServiceConfig

```yaml
managerConfig:
  replicaEvictionServiceConfig:
    schedulePeriodMins: 15
```
Configuration options controlling <tooltip term="replica">replica</tooltip> evictions from cache nodes due to 
expiration.

<deflist type="medium">
<include from="Config-options.md" element-id="schedule-period-mins"></include>
</deflist>

### replicaDeletionServiceConfig

```yaml
managerConfig:
  replicaDeletionServiceConfig:
    schedulePeriodMins: 15
```

Configuration options controlling <tooltip term="replica">replica</tooltip> deletion once expired.

<deflist type="medium">
<include from="Config-options.md" element-id="schedule-period-mins"></include>
</deflist>

### recoveryTaskAssignmentServiceConfig

```yaml
managerConfig:
  recoveryTaskAssignmentServiceConfig:
    schedulePeriodMins: 15
```

Configuration options controlling recovery tasks assignments to recovery nodes.

<deflist type="medium">
<include from="Config-options.md" element-id="schedule-period-mins"></include>
</deflist>

### snapshotDeletionServiceConfig

```yaml
managerConfig:
  snapshotDeletionServiceConfig:
    schedulePeriodMins: 15
    snapshotLifespanMins: 10080
```

Configuration options controlling <tooltip term="snapshot">snapshot</tooltip> deletion once expired.

<deflist type="full">
<include from="Config-options.md" element-id="schedule-period-mins"></include>
<def title="snapshotLifespanMins">

Configures how long a <tooltip term="snapshot">snapshot</tooltip> can exist before being deleted from S3. This must be 
set to a value larger than the
[managerConfig.replicaCreationServiceConfig.replicaLifespanMins](Config-options.md#replicacreationserviceconfig). When
this is larger than the `replicaLifespan` it enables restoring <tooltip term="replica">replicas</tooltip> from cold 
storage (see [managerConfig.replicaRestoreServiceConfig](Config-options.md#replicarestoreserviceconfig)). 
</def>
</deflist>

### replicaRestoreServiceConfig
<tldr>experimental</tldr>

```yaml
managerConfig:
  replicaRestoreServiceConfig:
    schedulePeriodMins: 15
    maxReplicasPerRequest: 200
    replicaLifespanMins: 60
    replicaSets: [rep1]
```

Configurations controlling on-demand restores for <tooltip term="snapshot">snapshots</tooltip> that exist that do not 
have corresponding <tooltip term="replica">replicas</tooltip>.

<deflist type="full">
<include from="Config-options.md" element-id="schedule-period-mins"></include>
<def title="maxReplicasPerRequest">
Maximum allowable <tooltip term="replica">replicas</tooltip> to be restored in a single request. When a request exceeds 
this value, and error will be returned to the user.
</def>
<def title="replicaLifespanMins">
How long the restored <tooltip term="replica">replica</tooltip> will exist before expiring.

See [indexerConfig.replicaCreationServiceConfig.replicaLifespanMins](Config-options.md#replicacreationserviceconfig)
</def>
<include from="Config-options.md" element-id="replica-sets"></include>
</deflist>

## clusterConfig
Cluster configuration options common to all node type.

```yaml
clusterConfig:
  clusterName: astra_local
  env: local
```

<deflist>
<def title="clusterName">
Unique name assigned to this cluster. Should be identical for all node types in the cluster, and is used for metrics
instrumentation.
</def>
<def title="env">
Environment string for this cluster. Should be identical for all node types deployed to a single environment, and is 
used for metrics instrumentation.
</def>
</deflist>

## recoveryConfig

Configuration options for the recovery indexer node.

### serverConfig {id=recovery-server-config}

```yaml
recoveryConfig:
  serverConfig:
    serverPort: 8081
    serverAddress: 10.0.100.4
    requestTimeoutMs: 10000
```

<include from="Config-options.md" element-id="server-config"></include>

### kafkaConfig {id=kafka-recovery}

```yaml
  kafkaConfig:
    kafkaTopic: test-topic
    kafkaTopicPartition: 0
    kafkaBootStrapServers: localhost:9092
    kafkaClientGroup: astra-test
    enableKafkaAutoCommit: true
    kafkaAutoCommitInterval: 5000
    kafkaSessionTimeout: 30000
    additionalProps: "{isolation.level: read_committed}"
```

<include from="Config-options.md" element-id="kafka-indexing-config"/>

## preprocessorConfig

Configuration options for the preprocessor node.

### kafkaConfig {id=kafka-preprocessor}
```yaml

preprocessorConfig:
  kafkaConfig:
    kafkaTopic: test-topic
    kafkaBootStrapServers: localhost:9092
    additionalProps: "{max.block.ms: 28500, linger.ms: 10, batch.size: 512000, buffer.memory: 1024000000, compression.type: snappy}"
```

<deflist>
<def title="kafkaTopic">
Kafka topic to produce messages to
</def>
<def title="kafkaBootStrapServers">
Address of the kafka server, or servers comma separated.
</def>
<def title="additionalProps">
String value of JSON encoded properties to set on Kafka producer. Any valid Kafka property can be used.
<a href="https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html">Conflent Kafka Producer Configuration Reference</a>
</def>
</deflist>

### schemaFile
```yaml
preprocessorConfig:
  schemaFile: schema.yaml
```
For valid formatting options refer to [Schema documentation.](Schema.md#schema-field-types)

### serverConfig {id=preprocessor-server-config}

```yaml
preprocessorConfig:
  serverConfig:
    serverPort: 8081
    serverAddress: 10.0.100.5
    requestTimeoutMs: 55000
```

<include from="Config-options.md" element-id="server-config"></include>

### preprocessorInstanceCount
```yaml
preprocessorConfig:
  preprocessorInstanceCount: 2
```
Indicates how many instances of the preprocessor are currently deployed. Used for scaling rate limiters such that each 
preprocessor instance will allow the `total rate limit / preprocessor instance count` through before applying.

### rateLimiterMaxBurstSeconds
```yaml
preprocessorConfig:
  rateLimiterMaxBurstSeconds: 1
```
Defines how many seconds rate limiting unused permits can be accumulated before no longer increasing. 

<tip>Must be greater than or equal to 1.</tip>

### rateLimitExceededErrorCode

```yaml
preprocessorConfig:
  rateLimitExceededErrorCode: 400
```

Error code to return when the rate limit of the preprocessor is exceeded. If using OpenSearch 
[Data Prepper](https://opensearch.org/docs/latest/data-prepper/) a return code of `400` or `404` would mark the request 
as unable to be retried and sent to the dead letter queue.
