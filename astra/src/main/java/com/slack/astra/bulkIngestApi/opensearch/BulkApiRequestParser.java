package com.slack.astra.bulkIngestApi.opensearch;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.schema.ReservedFields;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.writer.SpanFormatter;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.VersionType;
import org.opensearch.ingest.IngestDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class uses the Opensearch libraries to parse the bulk ingest request into documents which
 * can be inserted into Kafka. The goal of this is to leverage Opensearch where possible, while
 * preventing opensearch abstractions from leaking further into Astra.
 */
public class BulkApiRequestParser {

  private static final Logger LOG = LoggerFactory.getLogger(BulkApiRequestParser.class);

  private static final String SERVICE_NAME_KEY = "service_name";

  public static Map<String, List<Trace.Span>> parseRequest(
      byte[] postBody, Schema.IngestSchema schema) throws IOException {
    return convertIndexRequestToTraceFormat(parseBulkRequest(postBody), schema);
  }

  /**
   * We need users to be able to specify the timestamp field and unit. We rely on the user sending
   * us `@timestamp` in microseconds. If that doesn't happen, then we'll just use the time of
   * ingestion
   */
  public static long getTimestampFromIngestDocument(Map<String, Object> sourceAndMetadata) {
    if (sourceAndMetadata.containsKey(ReservedFields.TIMESTAMP)) {
      try {
        String dateString = String.valueOf(sourceAndMetadata.get(ReservedFields.TIMESTAMP));
        Instant instant = Instant.parse(dateString);
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
      } catch (Exception e) {
        LOG.warn(
            "Unable to parse timestamp from ingest document. Using current time as timestamp", e);
      }
    }

    // We tried parsing @timestamp fields and failed. Use the current time
    return ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
  }

  @VisibleForTesting
  public static Trace.Span fromIngestDocument(
      IngestDocument ingestDocument, Schema.IngestSchema schema) {

    Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();

    long timestampInMicros = getTimestampFromIngestDocument(sourceAndMetadata);

    // See https://blog.mikemccandless.com/2014/05/choosing-fast-unique-identifier-uuid.html on how
    // to improve this
    String id = null;
    if (sourceAndMetadata.get(IngestDocument.Metadata.ID.getFieldName()) != null) {
      String parsedId =
          String.valueOf(sourceAndMetadata.get(IngestDocument.Metadata.ID.getFieldName()));
      if (!parsedId.isEmpty()) {
        // only override the generated ID if it's not null, and not empty
        // this can still cause problems if a user provides duplicate values
        id = parsedId;
      }
    }

    if (id == null) {
      id = UUID.randomUUID().toString();
    }

    String index = "default";
    if (sourceAndMetadata.get(IngestDocument.Metadata.INDEX.getFieldName()) != null) {
      String parsedIndex =
          String.valueOf(sourceAndMetadata.get(IngestDocument.Metadata.INDEX.getFieldName()));
      if (!parsedIndex.isEmpty()) {
        index = parsedIndex;
      }
    }

    Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();
    spanBuilder.setId(ByteString.copyFrom(id.getBytes()));
    // Trace.Span proto expects duration in microseconds today
    spanBuilder.setTimestamp(timestampInMicros);

    if (sourceAndMetadata.get(LogMessage.ReservedField.PARENT_ID.fieldName) != null) {
      spanBuilder.setParentId(
          ByteString.copyFromUtf8(
              String.valueOf(sourceAndMetadata.get(LogMessage.ReservedField.PARENT_ID.fieldName))));
      sourceAndMetadata.remove(LogMessage.ReservedField.PARENT_ID.fieldName);
    }
    if (sourceAndMetadata.get(LogMessage.ReservedField.TRACE_ID.fieldName) != null) {
      spanBuilder.setTraceId(
          ByteString.copyFromUtf8(
              String.valueOf(sourceAndMetadata.get(LogMessage.ReservedField.TRACE_ID.fieldName))));
      sourceAndMetadata.remove(LogMessage.ReservedField.TRACE_ID.fieldName);
    }
    if (sourceAndMetadata.get(LogMessage.ReservedField.NAME.fieldName) != null) {
      spanBuilder.setName(
          String.valueOf(sourceAndMetadata.get(LogMessage.ReservedField.NAME.fieldName)));
      sourceAndMetadata.remove(LogMessage.ReservedField.NAME.fieldName);
    }
    if (sourceAndMetadata.get(LogMessage.ReservedField.DURATION.fieldName) != null) {
      try {
        spanBuilder.setDuration(
            Long.parseLong(
                sourceAndMetadata.get(LogMessage.ReservedField.DURATION.fieldName).toString()));
      } catch (NumberFormatException e) {
        LOG.warn(
            "Unable to parse duration={} from ingest document. Setting duration to 0",
            sourceAndMetadata.get(LogMessage.ReservedField.DURATION.fieldName));
        spanBuilder.setDuration(0);
      }
      sourceAndMetadata.remove(LogMessage.ReservedField.DURATION.fieldName);
    }

    // Remove the following internal metadata fields that OpenSearch adds
    sourceAndMetadata.remove(IngestDocument.Metadata.ROUTING.getFieldName());
    sourceAndMetadata.remove(IngestDocument.Metadata.VERSION.getFieldName());
    sourceAndMetadata.remove(IngestDocument.Metadata.VERSION_TYPE.getFieldName());

    // these fields don't need to be tags as they have been explicitly set already
    sourceAndMetadata.remove(IngestDocument.Metadata.ID.getFieldName());
    sourceAndMetadata.remove(IngestDocument.Metadata.INDEX.getFieldName());

    boolean tagsContainServiceName = false;
    for (Map.Entry<String, Object> kv : sourceAndMetadata.entrySet()) {
      if (!tagsContainServiceName && kv.getKey().equals(SERVICE_NAME_KEY)) {
        tagsContainServiceName = true;
      }
      List<Trace.KeyValue> tags =
          SpanFormatter.convertKVtoProto(kv.getKey(), kv.getValue(), schema);
      if (tags != null) {
        spanBuilder.addAllTags(tags);
      }
    }
    if (!tagsContainServiceName) {
      spanBuilder.addTags(
          Trace.KeyValue.newBuilder()
              .setKey(SERVICE_NAME_KEY)
              .setFieldType(Schema.SchemaFieldType.KEYWORD)
              .setVStr(index)
              .build());
    }

    return spanBuilder.build();
  }

  protected static Map<String, List<Trace.Span>> convertIndexRequestToTraceFormat(
      List<IndexRequest> indexRequests, Schema.IngestSchema schema) {
    // key - index. value - list of docs to be indexed
    Map<String, List<Trace.Span>> indexDocs = new HashMap<>();

    for (IndexRequest indexRequest : indexRequests) {
      String index = indexRequest.index();
      if (index == null) {
        continue;
      }
      IngestDocument ingestDocument = convertRequestToDocument(indexRequest);
      List<Trace.Span> docs = indexDocs.computeIfAbsent(index, key -> new ArrayList<>());
      docs.add(BulkApiRequestParser.fromIngestDocument(ingestDocument, schema));
    }
    return indexDocs;
  }

  // only parse IndexRequests
  @VisibleForTesting
  public static IngestDocument convertRequestToDocument(IndexRequest indexRequest) {
    String index = indexRequest.index();
    String id = indexRequest.id();
    String routing = indexRequest.routing();
    Long version = indexRequest.version();
    VersionType versionType = indexRequest.versionType();
    Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();

    return new IngestDocument(index, id, routing, version, versionType, sourceAsMap);

    // can easily expose Pipeline/CompoundProcessor(list of processors) that take an IngestDocument
    // and transform it
  }

  @VisibleForTesting
  public static List<IndexRequest> parseBulkRequest(byte[] postBody) throws IOException {
    List<IndexRequest> indexRequests = new ArrayList<>();
    BulkRequest bulkRequest = new BulkRequest();
    // calls parse under the hood
    bulkRequest.add(postBody, 0, postBody.length, null, MediaTypeRegistry.JSON);
    List<DocWriteRequest<?>> requests = bulkRequest.requests();
    for (DocWriteRequest<?> request : requests) {
      if (request.opType() == DocWriteRequest.OpType.INDEX
          | request.opType() == DocWriteRequest.OpType.CREATE) {

        // The client makes a DocWriteRequest and sends it to the server
        // IngestService#innerExecute is where the server eventually reads when request is an
        // IndexRequest. It then creates an IngestDocument
        indexRequests.add((IndexRequest) request);
      } else {
        LOG.warn("request={} of type={} not supported", request, request.opType().toString());
      }
    }
    return indexRequests;
  }
}
