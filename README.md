
# KalDB

KalDB is a cloud-native search engine for log management & analytics. It is designed to be very cost-effective, easy to operate, and scale to petabytes.

[Join our Slack community](https://join.slack.com/t/kaldb/shared_invite/zt-1om21f1yv-jyRUCH1JO6g6HMlKgd8mDw)

# 💡 Features

- Index data persisted on object storage(S3).
- No delayed logs: Prioritizes ingesting fresh data over older data.
- Dynamic schema: Ingest JSON documents with or without a strict schema
- Cloud-native: Kubernetes ready
- Plug-in for [Grafana UI](https://github.com/slackhq/slack-kaldb-app). 
- Designed to elastically scale to handle log spikes automatically.
- Based on Apache Lucene.
- Works out of the box with sensible defaults
- Built-in multi-tenancy so you don't have to run multiple clusters.
- Co-ordination free ingestion, so failure of a single node doesn't stop ingestion.
- Designed for zero data loss. 
- Distributed search
- Add and remove nodes in seconds. Elastically scale capacity up and down based on load.
- Decoupled compute & storage
- Ingest your documents with exactly-once semantics
- Kafka-native ingestion

# 🔎 Uses & Limitations
| :white_check_mark: &nbsp; When to use                                                  	                                                    | :x: When not to use                                       	            |
|---------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| Your documents are immutable: application logs, system logs, access logs, user actions logs, audit trail  (logs), etc.                    	 | Your documents are mutable.   	                                        |
| Your data has a time component. KalDB includes optimizations and design choices specifically related to time.                               | You need a low-latency search for e-commerce websites.               	 |
| You want a full-text search in a multi-tenant environment.     	                                                                            | You provide a public-facing search with high QPS.	                     |
| You want to index directly from Kafka.                                                                                                      | You want to re-score documents at query time.                          |
| You ingest a tremendous amount of logs and don't want to pay huge bills.                                                             	      |                                                                        |
| You ingest a tremendous amount of data and you don't want to waste your precious time babysitting your ElasticSearch cluster.               |                                                                        |

# 🎨  Design
![KalDB Architecture](docs/assets/images/kaldb_architecture.png)


# 💬 Community
## Talks
[KalDB: A k8s native log search platform](https://www.youtube.com/watch?v=soC04dpOQEM&t=9391s)

# 🙋 FAQ

### How is KalDB different from traditional search engines like Elasticsearch or Solr?
The core difference and advantage of KalDB is its architecture that is built from the ground up for cloud and log management. KalDB prioritizes indexing fresh logs over older logs, so your logs are never behind. Thanks to the true decoupled compute and storage, search instances are stateless, it is possible to auto-scale capacity within seconds. Last but not least, we implemented a highly-reliable distributed search and exactly-once semantics during indexing so that all engineers can sleep at night. To is cheaper than ES since we implement a tiered storage API, and on-demand restores.

# ⚡ Getting Started

To build the binary: `mvn clean package`

### Local development

> IntelliJ: Import the project as a Maven project.

IntelliJ run configs are provided for all node types, and execute using the provided `config/config.yaml`. These 
configurations are stored in the `.run` folder and should automatically be detected by IntelliJ upon importing the 
project.

To start KalDB and it's dependencies (Zookeeper, Kafka, S3) you can use the provided docker compose file:

```bash
docker-compose up
```

Index Data
1. Data from the  "test-topic-in" (preprocessorConfig/kafkaStreamConfig/upstreamTopics in config.yaml) Kafka topic is read as input by the preprocessor.
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