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
    serverAddress: localhost
    requestTimeoutMs: 5000
```
<snippet id="server-config">
<deflist>
<def title="serverPort">
</def>
<def title="serverAddress">
</def>
<def title="requestTimeoutMs">
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
```yaml
s3Config:
  s3AccessKey: access
  s3SecretKey: key
  s3Region: us-east-1
  s3EndPoint: localhost:9090
  s3Bucket: test-s3-bucket
  s3TargetThroughputGbps: 25
```

### s3AccessKey

### s3SecretKey

### s3Region

### s3EndPoint

### s3Bucket

### s3TargetThroughputGbps

## tracingConfig

``` yaml
tracingConfig:
  zipkinEndpoint: http://localhost:9411/api/v2/spans
  commonTags:
    clusterName: astra-local
    env: localhost
```

### zipkinEndpoint

### commonTags

Recommended common tags:
<deflist>
<def title="clusterName">
</def>
<def title="env">
</def>
</deflist>

## queryConfig

### serverConfig {id=query-server-config}

```yaml
queryConfig:
  serverConfig:
    serverPort: 8081
    serverAddress: localhost
    requestTimeoutMs: 5000
```

<include from="Config-options.md" element-id="server-config"></include>

### defaultQueryTimeout

### managerConnectString

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
</def>
<def title="zkPathPrefix">
</def>
<def title="zkSessionTimeoutMs">
</def>
<def title="zkConnectionTimeoutMs">
</def>
<def title="sleepBetweenRetriesMs">
</def>
</deflist>

## cacheConfig

### slotsPerInstance

### replicaSet

### dataDirectory {id=cache-data-directory}

### defaultQueryTimeoutMs {id=cache-default-query-timeout-ms}

### serverConfig {id=cache-server-config}

## managerConfig

### eventAggregationSecs

### scheduleInitialDelayMins

### serverConfig {id=manager-server-config}

<include from="Config-options.md" element-id="indexer-server-config"></include>

### replicaCreationServiceConfig

### replicaAssignmentServiceConfig

### replicaEvictionServiceConfig

### replicaDeletionServiceConfig

### recoveryTaskAssignmentServiceConfig

### snapshotDeletionServiceConfig

### replicaRestoreServiceConfig

## clusterConfig
```yaml
clusterConfig:
  clusterName: astra_local
  env: local
```

## recoveryConfig

### serverConfig {id=recovery-server-config}

<include from="Config-options.md" element-id="indexer-server-config"></include>

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
<warning><p><code>kafkaStreamConfig</code> is deprecated and unsupported</p></warning>

<deflist type="medium">
<def title="bootstrapServers">
DEPRECATED
</def>
<def title="applicationId">
DEPRECATED
</def>
<def title="numStreamThreads">
DEPRECATED
</def>
<def title="processingGuarantee">
DEPRECATED
</def>
<def title="additionalProps">
DEPRECATED
</def>
</deflist>

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

### upstreamTopics

### downstreamTopic

### preprocessorInstanceCount

### dataTransformer

### rateLimiterMaxBurstSeconds

### kafkaPartitionStickyTimeoutMs

### useBulkApi

### rateLimitExceededErrorCode