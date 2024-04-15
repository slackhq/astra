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
A cache node receives chunk assignments from the manager node, which are downloaded from S3.
</def>
<def title="MANAGER">
The manager node is responsible for cluster orchestration,
</def>
<def title="RECOVERY">
A recovery node indexes data that is skipped by an indexer, publishing snapshots.
</def>
<def title="PREPROCESSOR">
A preprocessor node serves a bulk ingest HTTP api, which is then transformed, rate limited, and sent to Kafka.
</def>
</deflist>


## indexerConfig

Configuration options for the indexer node.

### maxMessagesPerChunk
Maximum number of messages that are created per chunk before closing and uploading to S3. This should be roughly
equivalent to the `maxBytesPerChunk`, such that a rollover is triggered at roughly the same time regardless of 
messages or bytes.

```yaml
indexerConfig:
  maxMessagesPerChunk: 100000
```

### maxBytesPerChunk
Maximum bytes that are created per chunk before closing and uploading to S3. This should be roughly
equivalent to the `maxMessagesPerChunk`, such that a rollover is triggered at roughly the same time regardless of
messages or bytes.

```yaml
indexerConfig:
  maxBytesPerChunk: 1000000
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
How often Lucene commits to disk. This value will impact the accuracy of calculating the chunk size on disk for 
rollovers, so should be set conservatively to keep chunk sizes consistent. 
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

### dataDirectory {id=indexer-data-directory}

### maxOffsetDelayMessages

### defaultQueryTimeoutMs {id=indexer-default-query-timeout-ms}

### readFromLocationOnStart

### createRecoveryTasksOnStart

### maxChunksOnDisk

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
```

### zipkinEndpoint

Fully path to the Zipkin [POST spans endpoint](https://zipkin.io/zipkin-api/#/default/post_spans). Will be submitted as
a JSON array of span data.

### commonTags
Optional common tags to annotate on all submitted Zipkin traces. Can be overwritten by spans at runtime, if keys 
collide. 

<tip>Recommended common tags: <code>clusterName</code>, <code>env</code></tip>

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

### replicaSet

### dataDirectory {id=cache-data-directory}

### defaultQueryTimeoutMs {id=cache-default-query-timeout-ms}

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

### eventAggregationSecs

### scheduleInitialDelayMins

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

### replicaAssignmentServiceConfig

### replicaEvictionServiceConfig

### replicaDeletionServiceConfig

### recoveryTaskAssignmentServiceConfig

### snapshotDeletionServiceConfig

### replicaRestoreServiceConfig

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

### kafkaStreamConfig
```yaml
preprocessorConfig:
  kafkaStreamConfig:
    bootstrapServers: localhost:9092
    applicationId: astra_preprocessor
    numStreamThreads: 2
    processingGuarantee: at_least_once
    additionalProps: ""
```
<warning><code>kafkaStreamConfig</code> is deprecated and unsupported.</warning>

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

### upstreamTopics
```yaml
preprocessorConfig:
  upstreamTopics: ""
```
<warning><code>upstreamTopics</code> is deprecated.</warning>
<note>Should always be set to <code>""</code></note>

### downstreamTopic
```yaml
preprocessorConfig:
  downstreamTopic: ""
```
<warning><code>downstreamTopic</code> is deprecated.</warning>
<note>Should always be set to <code>""</code></note>

### preprocessorInstanceCount
```yaml
preprocessorConfig:
  preprocessorInstanceCount: 2
```
Indicates how many instances of the preprocessor are currently deployed. Used for scaling rate limiters such that each 
preprocessor instance will allow the `total rate limit / preprocessor instance count` through before applying.

### dataTransformer
<warning><code>dataTransformer</code> is deprecated.</warning>

```yaml
preprocessorConfig:
  dataTransformer: json
```
<note>Should always be set to <code>json</code></note>

### rateLimiterMaxBurstSeconds
```yaml
preprocessorConfig:
  rateLimiterMaxBurstSeconds: 1
```
Defines how many seconds rate limiting unused permits can be accumulated before no longer increasing. 

<tip>Must be greater than or equal to 1.</tip>

### kafkaPartitionStickyTimeoutMs

```yaml
preprocessorConfig:
  kafkaPartitionStickyTimeoutMs: 0
```

<warning><code>kafkaPartitionStickyTimeoutMs</code> is deprecated.</warning>
<note>Should always be set to <code>0</code></note>

### useBulkApi

```yaml
preprocessorConfig:
  useBulkApi: true
```
<warning><code>useBulkApi</code> is deprecated.</warning>
<note>Should always be set to <code>true</code></note>

Enable bulk ingest API, replacing the Kafka Streams API _(deprecated)_.

### rateLimitExceededErrorCode

```yaml
preprocessorConfig:
  rateLimitExceededErrorCode: 400
```

Error code to return when the rate limit of the preprocessor is exceeded. If using OpenSearch 
[Data Prepper](https://opensearch.org/docs/latest/data-prepper/) a return code of `400` or `404` would mark the request 
as unable to be retried and sent to the dead letter queue.
