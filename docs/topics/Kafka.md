# Kafka

## Troubleshooting

### `OffsetOutOfRangeException`

We have noticed an issue with Kafka related to the `OffsetOutOfRangeException`, when running the indexer in 
`isolation.level: read_committed`. This appears to be a known issue with Kafka and hung transactions (see 
[https://blog.digitalis.io/hung-kafka-transactions-3cdf9ca158ff](https://blog.digitalis.io/hung-kafka-transactions-3cdf9ca158ff), 
[https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense](https://blog.digitalis.io/hung-kafka-transactions-3cdf9ca158ff), 
[https://issues.apache.org/jira/browse/KAFKA-14402](https://blog.digitalis.io/hung-kafka-transactions-3cdf9ca158ff)). 
While originally [planned](https://cwiki.apache.org/confluence/display/KAFKA/Release+Plan+3.7.0) to be resolved in 
Kafka 3.7.0 this ended up [not shipping](https://downloads.apache.org/kafka/3.7.0/RELEASE_NOTES.html) in this version. 
As mentioned in resources above, if your issue is related to a hung transaction there are built-in Kafka scripts you 
can use to identify and resolve this issue, until a proper fix is shipped in a later version of Kafka.


<seealso>
    <category ref="external">
        <a href="https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html">Conflent Kafka Producer Configuration Reference</a>
    </category>
</seealso>