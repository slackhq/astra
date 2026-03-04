package com.slack.astra.bulkIngestApi;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser;
import com.slack.astra.proto.schema.Schema;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for defining the http endpoint behavior for the bulk ingest. It is
 * expected to handle appropriate rate limiting, error handling, and submit the parsed messages to
 * Kafka for ingestion.
 */
public class BulkIngestApi {
  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestApi.class);
  private final BulkIngestKafkaProducer bulkIngestKafkaProducer;
  private final DatasetRateLimitingService datasetRateLimitingService;
  private final MeterRegistry meterRegistry;
  private final Counter incomingByteTotal;
  private final Counter incomingDocsTotal;
  private final Timer bulkIngestTimer;
  private final String BULK_INGEST_INCOMING_BYTE_TOTAL = "astra_preprocessor_incoming_byte";
  private final String BULK_INGEST_INCOMING_BYTE_DOCS = "astra_preprocessor_incoming_docs";
  private final String BULK_INGEST_ERROR = "astra_preprocessor_error";
  private final String BULK_INGEST_TIMER = "astra_preprocessor_bulk_ingest";
  private final int rateLimitExceededErrorCode;
  private final Schema.IngestSchema schema;

  private final Counter bulkIngestErrorCounter;

  public BulkIngestApi(
      BulkIngestKafkaProducer bulkIngestKafkaProducer,
      DatasetRateLimitingService datasetRateLimitingService,
      MeterRegistry meterRegistry,
      int rateLimitExceededErrorCode,
      Schema.IngestSchema schema) {

    this.bulkIngestKafkaProducer = bulkIngestKafkaProducer;
    this.datasetRateLimitingService = datasetRateLimitingService;
    this.meterRegistry = meterRegistry;
    this.incomingByteTotal = meterRegistry.counter(BULK_INGEST_INCOMING_BYTE_TOTAL);
    this.incomingDocsTotal = meterRegistry.counter(BULK_INGEST_INCOMING_BYTE_DOCS);
    this.bulkIngestTimer = meterRegistry.timer(BULK_INGEST_TIMER);
    if (rateLimitExceededErrorCode <= 0 || rateLimitExceededErrorCode > 599) {
      this.rateLimitExceededErrorCode = 400;
    } else {
      this.rateLimitExceededErrorCode = rateLimitExceededErrorCode;
    }
    this.schema = schema;
    this.bulkIngestErrorCounter = meterRegistry.counter(BULK_INGEST_ERROR);
  }

  @Post("/_bulk")
  public HttpResponse addDocument(String bulkRequest) {
    // 1. Astra does not support the concept of "updates". It's always an add.
    // 2. The "index" is used as the span name
    String requestId = UUID.randomUUID().toString();
    LOG.info("Request ID: {}, Got _bulk request: {}", requestId, bulkRequest);
    CompletableFuture<HttpResponse> future = new CompletableFuture<>();
    Timer.Sample sample = Timer.start(meterRegistry);
    future.thenRun(() -> sample.stop(bulkIngestTimer));

    try {
      byte[] bulkRequestBytes = bulkRequest.getBytes(StandardCharsets.UTF_8);
      incomingByteTotal.increment(bulkRequestBytes.length);
      Map<String, List<Trace.Span>> docs = Map.of();
      try {
        LOG.info("Request ID: {}, parsing request", requestId);
        docs = BulkApiRequestParser.parseRequest(bulkRequestBytes, schema);
        LOG.info("Request ID: {}, finished parsing request", requestId);
      } catch (Exception e) {
        LOG.error("Request ID: {}, Request failed ", requestId, e);
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
        LOG.info("Request ID: {}, key size greater than 1", requestId);
        BulkIngestResponse response =
            new BulkIngestResponse(0, 0, "request must contain only 1 unique index");
        future.complete(HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response));
        bulkIngestErrorCounter.increment();
        return HttpResponse.of(future);
      }

      LOG.info("Request ID: {}, iterating through indexDocs of size: {}", requestId, docs.size());
      for (Map.Entry<String, List<Trace.Span>> indexDocs : docs.entrySet()) {
        incomingDocsTotal.increment(indexDocs.getValue().size());
        final String index = indexDocs.getKey();
        LOG.info("Request ID: {}, trying to acquire datasetRateLimitingService", requestId);
        if (!datasetRateLimitingService.tryAcquire(index, indexDocs.getValue())) {
          LOG.info("Request ID: {}, acquired datasetRateLimitingService", requestId);
          BulkIngestResponse response = new BulkIngestResponse(0, 0, "rate limit exceeded");
          future.complete(
              HttpResponse.ofJson(HttpStatus.valueOf(rateLimitExceededErrorCode), response));
          return HttpResponse.of(future);
        }
      }

      LOG.info("Request ID: {}, finished iterating through indexDocs of size: {}", requestId, docs.size());
      // todo - explore the possibility of using the blocking task executor backed by virtual
      // threads to fulfill this
      Map<String, List<Trace.Span>> finalDocs = docs;
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  LOG.info("Request ID: {}, submitting Kafka response", requestId);
                  BulkIngestResponse response =
                      bulkIngestKafkaProducer.submitRequest(finalDocs).getResponse();
                  LOG.info("Request ID: {}, submitted Kafka response", requestId);
                  future.complete(HttpResponse.ofJson(response));
                } catch (InterruptedException e) {
                  LOG.info("Request ID: {}, Request failed ", requestId, e);
                  bulkIngestErrorCounter.increment();
                  future.complete(
                      HttpResponse.ofJson(
                          INTERNAL_SERVER_ERROR, new BulkIngestResponse(0, 0, e.getMessage())));
                }
              });
    } catch (Exception e) {
      LOG.error("Request ID, {}, Request failed ", requestId, e);
      bulkIngestErrorCounter.increment();
      BulkIngestResponse response = new BulkIngestResponse(0, 0, e.getMessage());
      future.complete(HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response));
    }

    return HttpResponse.of(future);
  }
}
