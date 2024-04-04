# Development setup

## Docker compose
To bootstrap all required dependencies, execute in the root astra directory:
```bash
docker-compose up
```

## Services
* Zookeeper (port 2181)
* Kafka (port 9092)
* S3 (port 9090)
* Grafana (port 3000) 
    * http://localhost:3000/explore

## Troubleshooting

Interactive kafka message producer:
```bash
docker exec -it astra_kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic test-topic
```
