# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Building and Running

### Building the Project
```bash
# Build the entire project
mvn clean install

# Build without running tests
mvn clean install -DskipTests
```

### Running Tests
```bash
# Run all tests
mvn test

# Run a specific test
mvn test -Dtest=AstraTest

# Run a specific test method
mvn test -Dtest=AstraTest#testDistributedQueryOneIndexerOneQueryNode
```

### Code Formatting
```bash
# Format code using Google's style guide
mvn fmt:format
```

### Running Benchmarks
```bash
# Run all benchmarks
cd benchmarks
./jmh.sh

# Run specific benchmark
./jmh.sh IndexingBenchmark

# Run specific benchmark method
./jmh.sh IndexingBenchmark.measureIndexingAsKafkaSerializedDocument
```

## Development Environment

### Docker Compose
Astra is designed to be run with multiple components working together. The easiest way to set up a development environment is using the provided Docker Compose file:

```bash
# Start all services defined in docker-compose.yml
docker build -t slackhq/astra .
docker-compose up
```

This will start all required dependencies (Zookeeper, Kafka, S3Mock, OpenZipkin) and the Astra services (Preprocessor, Index, Manager, Query, Cache, Recovery).

### Configuration
Configuration is managed through YAML files and environment variables. The main configuration file is in `config/config.yaml`. All settings can be overridden by environment variables, as shown in the Docker Compose file.

To run Astra with a specific config:
```bash
java -jar astra/target/astra.jar /path/to/config.yaml
```

## Architecture Overview

Astra is a cloud-native search and analytics engine for log, trace, and audit data, built on Apache Lucene. The system consists of multiple services that work together:

1. **Preprocessor** (port 8086): Handles data ingestion, schema validation, and writes to Kafka
2. **Indexer** (port 8080): Consumes data from Kafka and builds Lucene indexes
3. **Manager** (port 8083): Coordinates between components and handles metadata
4. **Query** (port 8081): Provides API endpoints for searching data
5. **Cache** (port 8082): Manages cached replicas for faster queries
6. **Recovery** (port 8085): Handles data recovery operations

Each component can be run separately using the `NODE_ROLES` configuration option, which makes Astra horizontally scalable.

### Data Flow:
1. Data is ingested through Preprocessor (or directly to Kafka)
2. Indexer consumes data from Kafka and builds Lucene indexes
3. Data is stored in chunks which can be persisted to S3
4. Manager coordinates replica creation and assignment
5. Query service handles search requests, distributing them across nodes
6. Cache service provides faster access to frequently accessed data

### Key Components:
- **ChunkManager**: Manages data chunks (IndexingChunkManager, CachingChunkManager, RecoveryChunkManager)
- **MetadataStore**: ZooKeeper-based metadata storage for datasets, replicas, schemas, etc.
- **ArmeriaService**: HTTP/gRPC server for all components
- **BlobStore**: Interface for S3 storage
- **FieldRedaction**: Manages field-level redaction for sensitive data

## API Compatibility
- Astra provides OpenSearch/Elasticsearch API compatibility for easy integration with tools like Grafana
- Zipkin API for tracing support

## Testing
Astra has comprehensive unit tests and integration tests. The main classes involved in testing are:
- TestKafkaServer and TestingZKServer for local test dependencies
- AstraTestExecutionListener for JUnit test setup
- S3MockExtension for S3 mock testing