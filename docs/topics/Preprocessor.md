# Preprocessor

The preprocessor nodes are responsible for consuming from an upstream (http ingest) and sending
appropriately formatted messages to the indexers. The preprocessors apply rate-limiting and service assignments, as
well as ensuring the messages sent to the indexers are all the same message format.

## System design
The preprocessor is a component which reads the incoming logs and prepare the logs for ingestion. It performs the
following functions:

* Ensure logs are well-formed and valid - including data validation checks for data types and timestamps
* Apply any per-service rate limits on logs
* Format the logs to be ready for indexing
* Partition the logs into specific partitions in Kafka so they can be consumed by the indexers

## Basic operation
### Build & deploy
The preprocessor nodes can be deployed quickly, increasing or decreasing in size as needed to accommodate upstream
throughput. Long term this component will likely be set to autoscale.

### Adding capacity
Capacity can be added to the preprocessors by increasing their count, and load will automatically be distributed
across the instances. When increasing the instance count you will need to also increase the 
[preprocessorConfig.preprocessorInstanceCount](Config-options.md#preprocessorinstancecount), as that is used when 
calculating the per-instance scaled throughput.

<warning>
<p>Make sure the <code>preprocessorInstanceCount</code> matches the deployed replica count.</p>

```yaml
preprocessorConfig:
  preprocessorInstanceCount: 2
```
</warning>

