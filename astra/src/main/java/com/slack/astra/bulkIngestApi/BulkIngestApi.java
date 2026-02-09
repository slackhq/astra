package com.slack.astra.bulkIngestApi;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;

import com.google.common.annotations.VisibleForTesting;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for defining the http endpoint behavior for the bulk ingest. It is
 * expected to handle appropriate rate limiting, error handling, and submit the parsed messages to
 * Kafka for ingestion.
 */
public class BulkIngestApi {
  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestApi.class);

  public static final String ASTRA_SCHEMA_ENFORCEMENT_FLAG = "astra.schema.enforcement.enabled";

  private final BulkIngestKafkaProducer bulkIngestKafkaProducer;
  private final DatasetRateLimitingService datasetRateLimitingService;
  private final Schema.IngestSchema schema;
  private final DatasetSchemaService datasetSchemaService;
  private final MeterRegistry meterRegistry;
  private final Counter incomingByteTotal;
  private final Counter incomingDocsTotal;
  private final Timer bulkIngestTimer;
  private final String BULK_INGEST_INCOMING_BYTE_TOTAL = "astra_preprocessor_incoming_byte";
  private final String BULK_INGEST_INCOMING_BYTE_DOCS = "astra_preprocessor_incoming_docs";
  private final String BULK_INGEST_ERROR = "astra_preprocessor_error";
  private final String BULK_INGEST_TIMER = "astra_preprocessor_bulk_ingest";
  private final int rateLimitExceededErrorCode;

  private final Counter bulkIngestErrorCounter;

  /** Original constructor — uses a static schema for type-aware parsing. */
  public BulkIngestApi(
      BulkIngestKafkaProducer bulkIngestKafkaProducer,
      DatasetRateLimitingService datasetRateLimitingService,
      MeterRegistry meterRegistry,
      int rateLimitExceededErrorCode,
      Schema.IngestSchema schema) {
    this(
        bulkIngestKafkaProducer,
        datasetRateLimitingService,
        meterRegistry,
        rateLimitExceededErrorCode,
        schema,
        null);
  }

  /** Schema enforcement constructor — uses DatasetSchemaService for dynamic schema + filtering. */
  public BulkIngestApi(
      BulkIngestKafkaProducer bulkIngestKafkaProducer,
      DatasetRateLimitingService datasetRateLimitingService,
      MeterRegistry meterRegistry,
      int rateLimitExceededErrorCode,
      DatasetSchemaService datasetSchemaService) {
    this(
        bulkIngestKafkaProducer,
        datasetRateLimitingService,
        meterRegistry,
        rateLimitExceededErrorCode,
        null,
        datasetSchemaService);
  }

  private BulkIngestApi(
      BulkIngestKafkaProducer bulkIngestKafkaProducer,
      DatasetRateLimitingService datasetRateLimitingService,
      MeterRegistry meterRegistry,
      int rateLimitExceededErrorCode,
      Schema.IngestSchema schema,
      DatasetSchemaService datasetSchemaService) {

    this.bulkIngestKafkaProducer = bulkIngestKafkaProducer;
    this.datasetRateLimitingService = datasetRateLimitingService;
    this.schema = schema;
    this.datasetSchemaService = datasetSchemaService;
    this.meterRegistry = meterRegistry;
    this.incomingByteTotal = meterRegistry.counter(BULK_INGEST_INCOMING_BYTE_TOTAL);
    this.incomingDocsTotal = meterRegistry.counter(BULK_INGEST_INCOMING_BYTE_DOCS);
    this.bulkIngestTimer = meterRegistry.timer(BULK_INGEST_TIMER);
    if (rateLimitExceededErrorCode <= 0 || rateLimitExceededErrorCode > 599) {
      this.rateLimitExceededErrorCode = 400;
    } else {
      this.rateLimitExceededErrorCode = rateLimitExceededErrorCode;
    }
    this.bulkIngestErrorCounter = meterRegistry.counter(BULK_INGEST_ERROR);
  }

  @Post("/_bulk")
  public HttpResponse addDocument(String bulkRequest) {
    // 1. Astra does not support the concept of "updates". It's always an add.
    // 2. The "index" is used as the span name
    CompletableFuture<HttpResponse> future = new CompletableFuture<>();
    Timer.Sample sample = Timer.start(meterRegistry);
    future.thenRun(() -> sample.stop(bulkIngestTimer));

    try {
      byte[] bulkRequestBytes = bulkRequest.getBytes(StandardCharsets.UTF_8);
      incomingByteTotal.increment(bulkRequestBytes.length);
      Map<String, List<Trace.Span>> docs = Map.of();
      try {
        if (Boolean.getBoolean(ASTRA_SCHEMA_ENFORCEMENT_FLAG) && datasetSchemaService != null) {
          // Get schema from the schema service (dynamically updated from SchemaMetadataStore)
          Schema.IngestSchema enforcedSchema = datasetSchemaService.getSchema();
          AstraConfigs.SchemaMode schemaMode = datasetSchemaService.getSchemaMode();

          // Step 1: Parse the request with schema-aware type conversion
          docs = BulkApiRequestParser.parseRequest(bulkRequestBytes, enforcedSchema);
          // Step 2: Apply filtering based on schema mode
          docs = filterDocsBySchemaMode(docs, enforcedSchema, schemaMode);
        } else {
          docs = BulkApiRequestParser.parseRequest(bulkRequestBytes, schema);
        }
      } catch (Exception e) {
        LOG.error("Request failed ", e);
        bulkIngestErrorCounter.increment();
        BulkIngestResponse response = new BulkIngestResponse(0, 0, e.getMessage());
        future.complete(HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response));
      }

      // todo - our rate limiter doesn't have a way to acquire permits across multiple
      // datasets
      // so today as a limitation we reject any request that has documents against
      // multiple indexes
      // We think most indexing requests will be against 1 index
      if (docs.keySet().size() > 1) {
        BulkIngestResponse response =
            new BulkIngestResponse(0, 0, "request must contain only 1 unique index");
        future.complete(HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response));
        bulkIngestErrorCounter.increment();
        return HttpResponse.of(future);
      }

      for (Map.Entry<String, List<Trace.Span>> indexDocs : docs.entrySet()) {
        incomingDocsTotal.increment(indexDocs.getValue().size());
        final String index = indexDocs.getKey();
        if (!datasetRateLimitingService.tryAcquire(index, indexDocs.getValue())) {
          BulkIngestResponse response = new BulkIngestResponse(0, 0, "rate limit exceeded");
          future.complete(
              HttpResponse.ofJson(HttpStatus.valueOf(rateLimitExceededErrorCode), response));
          return HttpResponse.of(future);
        }
      }

      // todo - explore the possibility of using the blocking task executor backed by virtual
      // threads to fulfill this
      Map<String, List<Trace.Span>> finalDocs = docs;
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  BulkIngestResponse response =
                      bulkIngestKafkaProducer.submitRequest(finalDocs).getResponse();
                  future.complete(HttpResponse.ofJson(response));
                } catch (InterruptedException e) {
                  LOG.error("Request failed ", e);
                  bulkIngestErrorCounter.increment();
                  future.complete(
                      HttpResponse.ofJson(
                          INTERNAL_SERVER_ERROR, new BulkIngestResponse(0, 0, e.getMessage())));
                }
              });
    } catch (Exception e) {
      LOG.error("Request failed ", e);
      bulkIngestErrorCounter.increment();
      BulkIngestResponse response = new BulkIngestResponse(0, 0, e.getMessage());
      future.complete(HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response));
    }

    return HttpResponse.of(future);
  }

  /**
   * Filters documents based on the schema mode. In DROP_UNKNOWN mode, removes tags that are not
   * defined in the schema.
   */
  @VisibleForTesting
  public static Map<String, List<Trace.Span>> filterDocsBySchemaMode(
      Map<String, List<Trace.Span>> docs,
      Schema.IngestSchema schema,
      AstraConfigs.SchemaMode schemaMode) {
    if (schemaMode != AstraConfigs.SchemaMode.SCHEMA_MODE_DROP_UNKNOWN) {
      return docs;
    }

    Map<String, List<Trace.Span>> filteredDocs = new HashMap<>();
    for (Map.Entry<String, List<Trace.Span>> entry : docs.entrySet()) {
      List<Trace.Span> filteredSpans =
          entry.getValue().stream()
              .map(span -> filterSpanTags(span, schema))
              .collect(Collectors.toList());
      filteredDocs.put(entry.getKey(), filteredSpans);
    }
    return filteredDocs;
  }

  /** Filters tags from a span, keeping only those defined in the schema. */
  @VisibleForTesting
  public static Trace.Span filterSpanTags(Trace.Span span, Schema.IngestSchema schema) {
    List<Trace.KeyValue> filteredTags = new ArrayList<>();
    for (Trace.KeyValue tag : span.getTagsList()) {
      String key = tag.getKey();
      // For multi-field tags like "field.keyword", check the base field name
      String baseKey = key.contains(".") ? key.substring(0, key.indexOf('.')) : key;
      if (schema.containsFields(baseKey)) {
        filteredTags.add(tag);
      }
    }

    return span.toBuilder().clearTags().addAllTags(filteredTags).build();
  }
}
