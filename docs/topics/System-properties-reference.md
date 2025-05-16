# System properties reference

This is a reference of all available system properties that can be set for an Astra application, and recommended values.

### astra.bulkIngest.producerSleepMs
<tldr>experimental</tldr>
How long Astra will wait before starting the next bulk batch if the last batch was empty.

```bash
-Dastra.bulkIngest.producerSleepMs=50
```

{style="narrow"}
defaultValue
: 50

### astra.bulkIngest.useKafkaTransactions
<tldr>experimental</tldr>
The preprocessor can write documents to kafka using transactions.
Enable this to ensure that all documents are written using exactly once semantics 
and all kafka brokers for a partition acknowledge a write. Defaults to false.

```bash
-Dastra.bulkIngest.useKafkaTransactions=true
```

## astra.concurrent.query
<tldr>experimental</tldr>
The amount of concurrent queries that are permitted at the application level.

```bash
-Dastra.concurrent.query=4
```

{style="narrow"}
defaultValue
: ```java
Runtime.getRuntime().availableProcessors() - 1
```

## astra.s3CrtBlobFs.maxNativeMemoryLimitBytes
<tldr>experimental</tldr>
Sets the max allowable off-heap consumption of the AWS CRT client.

```bash
-Dastra.s3CrtBlobFs.maxNativeMemoryLimitBytes=1073741824
```

{style="narrow"}
defaultValue
: ```java
// Default to 5% of the heap size for the max crt off-heap or 1GiB (min for client)
long jvmMaxHeapSizeBytes = Runtime.getRuntime().maxMemory();
long defaultCrtMemoryLimit = Math.max(Math.round(jvmMaxHeapSizeBytes * 0.05), 1073741824);
```

## astra.recovery.maxRolloverMins
<tldr>experimental</tldr>
Maximum allowable time before a recovery task is failed and reintroduced into the processing pipeline. 

```bash
-Dastra.recovery.maxRolloverMins=90
```

{style="narrow"}
defaultValue
: 90

## astra.query.schemaTimeoutMs
<tldr>experimental</tldr>
Maximum amount of time before timing out individual indexer schema queries and returning the aggregate of returned 
schema.

{style="narrow"}
defaultValue
: 500


## astra.mapping.totalFieldsLimit
<tldr>experimental</tldr>
Maximum amount of total fields used by the mapper service for query parsing.

{style="narrow"}
defaultValue
: 2500

## astra.mapping.dynamicFieldsLimit
<tldr>experimental</tldr>
Maximum amount of dynamic fields indexed in indexer when building a schema.

Should be lower than astra.mapping.totalFieldsLimit to account for required fields and schema fields.

{style="narrow"}
defaultValue
: 1500
