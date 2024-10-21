package com.slack.astra.writer;

import com.slack.astra.chunkManager.ChunkManager;
import com.slack.astra.logstore.LogMessage;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A LogMessageWriter ingests ConsumerRecords into a ChunkManager.
 *
 * <p>The current generic ingestion pipeline can index any json message as follows: ConsumerRecord
 * -> JSON -> LogWireMessage -> LogMessage
 *
 * <p>We ingest a JSON blob from Kafka and parse it into a LogWireMessage. LogWireMessage needs an
 * id, an index name and a json map. LogMessage in addition ensures that the index name is valid,
 * has a timestamp and the map known fields contain valid types.
 *
 * <p>While this format is generic the format has a few drawbacks compared to Spans. Spans are a
 * better meta-format for logs since they standardize the basic fields, make logs more service
 * centric instead of index centric. So, an ideal indexer ingestion pipeline would be:
 * ConsumerRecord -> ListOfSpans -> Span -> LuceneDocument.
 *
 * <p>However, at this point we can't presume that all the logs will be in span format. So, we want
 * to support both the json logs and spans in the ingestion pipeline. Since a JSON Blob is more
 * basic than a span, we will convert all the spans into LogMessage. So, the current pipeline would
 * look as follows: ConsumerRecord -> f(ConsumerRecord -> List<LogMessage>) -> LogMessage
 *
 * <p>At Slack, we also wrap our json blobs and spans in an additional MurronMessage wrapper. So,
 * the transformation function for api_log whose log messages are json blobs: ConsumerRecord ->
 * MurronMessage -> Span -> List<Span> -> LogMessage. For traces topic which contains a list of
 * spans, the transformation function would look as follows: ConsumerRecord -> MurronMessage ->
 * List<Span> -> List<LogMessage>.
 *
 * <p>In future, when all the logs are written only as spans, we can directly convert a span to a
 * Lucene Document, obviating the need for MurronMessage and LogMessage wrappers. Meanwhile, we pass
 * in a data transformation function to this class as input so we can abstract away the specific
 * details of the message format from the indexer.
 *
 * <p>In the long term, we want to index only spans since spans offer several advantages over basic
 * logs like standardization, provide a service centric log view, ability to ingest and query logs
 * and traces the same way etc..
 *
 * <p>TODO: In future, implement MessageWriter interfacce on ChunkManager.
 */
public class LogMessageWriterImpl implements MessageWriter {

  private final ChunkManager<LogMessage> chunkManager;

  public LogMessageWriterImpl(ChunkManager<LogMessage> chunkManager) {
    this.chunkManager = chunkManager;
  }

  @Override
  public boolean insertRecord(ConsumerRecord<String, byte[]> record) throws IOException {
    if (record == null) return false;

    // Currently, ChunkManager.addMessage increments a failure counter to indicate an ingestion
    // error. We decided to throw the exception to a higher level since in a batch ingestion
    // the upper layers of the stack can't take any further action. If this becomes an issue
    // in future, propagate the exception upwards here or return a value.
    chunkManager.addMessage(
        Trace.Span.parseFrom(record.value()),
        record.serializedValueSize(),
        String.valueOf(record.partition()),
        record.offset(),
        false);
    return true;
  }
}
