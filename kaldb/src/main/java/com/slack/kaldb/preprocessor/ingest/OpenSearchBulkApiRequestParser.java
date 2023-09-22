package com.slack.kaldb.preprocessor.ingest;

import com.google.protobuf.ByteString;
import com.slack.kaldb.writer.SpanFormatter;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.VersionType;
import org.opensearch.ingest.IngestDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchBulkApiRequestParser {

  public static final Logger LOG = LoggerFactory.getLogger(OpenSearchBulkApiRequestParser.class);

  private static String SERVICE_NAME_KEY = "service_name";

  public static Trace.Span fromIngestDocument(IngestDocument ingestDocument) {
    ZonedDateTime timestamp =
        (ZonedDateTime)
            ingestDocument
                .getIngestMetadata()
                .getOrDefault("timestamp", ZonedDateTime.now(ZoneOffset.UTC));

    Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
    String id = (String) sourceAndMetadata.get(IngestDocument.Metadata.ID.getFieldName());
    // See https://blog.mikemccandless.com/2014/05/choosing-fast-unique-identifier-uuid.html on how
    // to improve this
    if (id == null) {
      id = UUID.randomUUID().toString();
    }
    String index = (String) sourceAndMetadata.get(IngestDocument.Metadata.INDEX.getFieldName());

    Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();
    spanBuilder.setId(ByteString.copyFromUtf8(id));
    // Trace.Span proto expects duration in microseconds today
    spanBuilder.setTimestamp(
        TimeUnit.MICROSECONDS.convert(timestamp.toInstant().toEpochMilli(), TimeUnit.MILLISECONDS));

    // Remove the following internal metadata fields that OpenSearch adds
    sourceAndMetadata.remove(IngestDocument.Metadata.ROUTING.getFieldName());
    sourceAndMetadata.remove(IngestDocument.Metadata.VERSION.getFieldName());
    sourceAndMetadata.remove(IngestDocument.Metadata.VERSION_TYPE.getFieldName());
    // these two fields don't need to be tags as they have been explicitly set already
    sourceAndMetadata.remove(IngestDocument.Metadata.ID.getFieldName());
    sourceAndMetadata.remove(IngestDocument.Metadata.INDEX.getFieldName());

    sourceAndMetadata.forEach(
        (key, value) -> spanBuilder.addTags(SpanFormatter.convertKVtoProto(key, value)));
    spanBuilder.addTags(
        Trace.KeyValue.newBuilder()
            .setKey(SERVICE_NAME_KEY)
            .setVType(Trace.ValueType.STRING)
            .setVStr(index)
            .build());
    return spanBuilder.build();
  }

  // key - index. value - list of docs to be indexed
  public static Map<String, List<Trace.Span>> convertIndexRequestToTraceFormat(
      List<IndexRequest> indexRequests) {
    Map<String, List<Trace.Span>> indexDocs = new HashMap<>();

    for (IndexRequest indexRequest : indexRequests) {
      String index = indexRequest.index();
      if (index == null) {
        continue;
      }
      IngestDocument ingestDocument = convertRequestToDocument(indexRequest);
      List<Trace.Span> docs = indexDocs.computeIfAbsent(index, key -> new ArrayList<>());
      docs.add(OpenSearchBulkApiRequestParser.fromIngestDocument(ingestDocument));
    }
    return indexDocs;
  }

  // only parse IndexRequests
  protected static IngestDocument convertRequestToDocument(IndexRequest indexRequest) {
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

  public static List<IndexRequest> parseBulkRequest(String postBody) throws IOException {
    List<IndexRequest> indexRequests = new ArrayList<>();
    BulkRequest bulkRequest = new BulkRequest();
    // calls parse under the hood
    bulkRequest.add(
        postBody.getBytes(StandardCharsets.UTF_8), 0, postBody.length(), null, XContentType.JSON);
    List<DocWriteRequest<?>> requests = bulkRequest.requests();
    for (DocWriteRequest<?> request : requests) {
      if (request.opType() == DocWriteRequest.OpType.INDEX) {

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
