# KalDB

KalDB is a cloud-native search engine for log management & analytics. It is designed to be very cost-effective, easy to operate, and scale to petabytes.

# 💡 Features

- Index data persisted on object storage(S3).
- No delayed logs: Prioritizes ingesting fresh data over older data.
- Ingest JSON documents with or without a strict schema
- Plug-in for [Grafana UI](https://github.com/slackhq/slack-kaldb-app). 
- Designed to elastically scale to handle log spikes automatically.
- Based on Apache Lucene.
- Works out of the box with sensible defaults
- Built-in multi-tenancy so you don't have to run multiple clusters.
- Co-ordination free ingestion, so failure of a single node doesn't stop ingestion.
- Designed for zero data loss. 
- Distributed search
- Cloud-native: Kubernetes ready
- Add and remove nodes in seconds
- Decoupled compute & storage
- Ingest your documents with exactly-once semantics
- Kafka-native ingestion

# 🔎 Uses & Limitations
| :white_check_mark: &nbsp; When to use                                                  	                                                    | :x: When not to use                                       	|
|---------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------	|
| Your documents are immutable: application logs, system logs, access logs, user actions logs, audit trail  (logs), etc.                    	 | Your documents are mutable.   	|
| Your data has a time component. KalDB includes optimizations and design choices specifically related to time.                               | You need a low-latency search for e-commerce websites.               	|
| You want a full-text search in a multi-tenant environment.     	                                                                            | You provide a public-facing search with high QPS.	|
| You want to index directly from Kafka.                                                                                                      | You want to re-score documents at query time.
| You ingest a tremendous amount of logs and don't want to pay huge bills.                                                             	      |
| You ingest a tremendous amount of data and you don't want to waste your precious time babysitting your ElasticSearch cluster.                             

#⚡ Getting Started

To build the binary: `mvn clean package`

## Local development

> IntelliJ: Import the project as a Maven project.

IntelliJ run configs are provided for all node types, and execute using the provided `config/config.yaml`. These 
configurations are stored in the `.run` folder and should automatically be detected by IntelliJ upon importing the 
project.

To start the application dependencies (Zookeeper, Kafka, S3) you can use the provided docker compose file:
```bash
docker-compose up
```
