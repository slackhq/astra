# Getting started

> IntelliJ: Import the project as a Maven project.

IntelliJ run configs are provided for all node types, and execute using the provided `config/config.yaml`. These
configurations are stored in the `.run` folder and should automatically be detected by IntelliJ upon importing the
project.

## Quick start

1. Build and run docker compose to bring up dependencies and Astra nodes
```bash
docker build -t slackhq/astra .

docker compose up
```

2. In Kafka container terminal, create input topic (preprocessor crashes if it does not exist before configuring manager in next step)
```bash
kafka-topics.sh --create --topic test-topic-in --bootstrap-server localhost:9092
```

3. Run 2 curl commands to configure 1 partition

```bash
curl -XPOST -H 'content-type: application/json; charset=utf-8; protocol=gRPC' http://localhost:8083'/slack.proto.kaldb.ManagerApiService/CreateDatasetMetadata' -d '{
  "name": "test",
  "owner": "test@email.com",
  "serviceNamePattern": "_all"
}'

curl -XPOST -H 'content-type: application/json; charset=utf-8; protocol=gRPC' http://localhost:8083'/slack.proto.kaldb.ManagerApiService/UpdatePartitionAssignment' -d '{
  "name": "test",
  "throughputBytes": "4000000",
  "partitionIds": ["0"]
}'
```
This can optionally be achieved in the manager UI at [http://localhost:8083/docs](http://localhost:8083/docs)

4. Add logs via bulk ingest
```bash
curl --location 'http://localhost:8086/_bulk' \
--header 'Content-type: application/x-ndjson' \
--data '{ "index" : { "_index" : "test", "_id" : "100" } }
{ "@timestamp": "2024-03-07T12:00:00.000Z", "level": "INFO", "message": "This is a log message", "service-name": "test" }
'
```

5. Example curl to read data
Note: This is similar to [ES _msearch](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/search-multi-search.html) but `size`, `lte`, and `gte` are all currently required.

```bash
curl --location 'http://localhost:8081/_msearch' \
--header 'Content-type: application/x-ndjson' \
--data '{ "index": "test"}
{"query" : {"match_all" : {}, "gte":1625156649889,"lte":2708540790265}, "size": 500}
'
```

Query via Grafana
```
http://localhost:3000/explore
```