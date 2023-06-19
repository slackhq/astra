
# KalDB
[![release version](https://img.shields.io/github/v/release/slackhq/kaldb?include_prereleases)](https://github.com/slackhq/kaldb/releases)
[![release pipeline](https://img.shields.io/github/actions/workflow/status/slackhq/kaldb/maven.yml?branch=master)](https://github.com/slackhq/kaldb/actions/workflows/maven.yml)
[![license](https://img.shields.io/github/license/slackhq/kaldb)](https://github.com/slackhq/kaldb/blob/master/LICENSE)

KalDB is a cloud-native search and analytics engine for log, trace, and audit data. It is designed to easy to operate, 
cost-effective, and scale to petabytes of data.

## Goals
- Native support for log, trace, audit use cases.
- Aggressively prioritize ingest of recent data over older data.
- Full-text search capability.
- First-class Kubernetes support for all components.
- Autoscaling of ingest and query capacity.
- Coordination free ingestion, so failure of a single node does not impact ingestion.
- Works out of the box with sensible defaults.
- Designed for zero data loss.
- First-class Grafana support with [accompanying plugin](https://github.com/slackhq/slack-kaldb-app).
- Built-in multi-tenancy, supporting several small use-cases on a single cluster.
- Supports the majority of Apache Lucene features.
- Drop-in replacement for most Opensearch log use cases.

## Non-Goals
- General-purpose search cases, such as for an ecommerce site.
- Document mutability - records are expected to be append only.
- Additional storage engines other than Lucene.
- Support for JVM versions other than the current LTS.
- Supporting multiple Lucene versions. 

## Quick Start

> IntelliJ: Import the project as a Maven project.

IntelliJ run configs are provided for all node types, and execute using the provided `config/config.yaml`. These
configurations are stored in the `.run` folder and should automatically be detected by IntelliJ upon importing the
project.

To start KalDB and it's dependencies (Zookeeper, Kafka, S3) you can use the provided docker compose file:

```bash
docker-compose up
```

Index Data
1. Data from the "test-topic-in" (preprocessorConfig/kafkaStreamConfig/upstreamTopics in config.yaml) Kafka topic is read as input by the preprocessor.
2. The input data transformer "json" (preprocessorConfig/dataTransformer in config.yaml) is how the preprocessor will parse the data.
3. Each document must contain 2 mandatory fields - "service_name" and "timestamp" (DateTimeFormatter.ISO_INSTANT)
4. There needs to be a dataset entry for the incoming data that maps the incoming service name
5. To create a dataset entry, go to the manager node (default http://localhost:8083/docs) and call CreateDatasetMetadata with name/owner as "test" and serviceNamePattern = "_all"
6. Then we need to update partition assignment. For this we have to go to the manager node (default http://localhost:8083/docs) and call UpdatePartitionAssignment with name="test", throughputBytes=1000000 (1 MB/s after which messages will be dropped) and partitionIds=["0"] (the partition is a string and here we tell to only read from partition 0 of test-topic-in)
7. Now we can start producing data to Kafka partiton=0 partition="test-topic-in"
8. The preprocessor writes data into the following kafka topic "test-topic"(preprocessorConfig/downstreamTopic in config.yaml). We apply rate-limits etc.
9. The indexer service is configured to read from "test-topic" (indexerConfig/kafkaConfig/kafkaTopic in config.yaml) and creates lucene indexes locally

Query via Grafana
```
http://localhost:3000/explore
```

## Contributing
If you are interested in reporting/fixing issues and contributing directly to the code base, please see [CONTRIBUTING](.github/CONTRIBUTING.md) for more information on what we're looking for and how to get started.

## Community
[Join our Slack community](https://join.slack.com/t/kaldb/shared_invite/zt-1om21f1yv-jyRUCH1JO6g6HMlKgd8mDw)
### Presentations
[KalDB: A k8s native log search platform](https://www.youtube.com/watch?v=soC04dpOQEM&t=9391s)

## Licensing
Licensed under [MIT](LICENSE). Copyright (c) 2021 Slack.
