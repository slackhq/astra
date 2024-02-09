package com.slack.kaldb.bulkIngestApi;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.kaldb.bulkIngestApi.opensearch.BulkApiRequestParser;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
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
  private final String BULK_INGEST_INCOMING_BYTE_TOTAL = "kaldb_preprocessor_incoming_byte";
  private final String BULK_INGEST_INCOMING_BYTE_DOCS = "kaldb_preprocessor_incoming_docs";
  private final String BULK_INGEST_TIMER = "kaldb_preprocessor_bulk_ingest";
  private final int rateLimitExceededErrorCode;

  public BulkIngestApi(
      BulkIngestKafkaProducer bulkIngestKafkaProducer,
      DatasetRateLimitingService datasetRateLimitingService,
      MeterRegistry meterRegistry,
      int rateLimitExceededErrorCode) {

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
  }

  @Post("/_bulk")
  public HttpResponse addDocument(String bulkRequest) {
    // 1. Kaldb does not support the concept of "updates". It's always an add.
    // 2. The "index" is used as the span name
    CompletableFuture<HttpResponse> future = new CompletableFuture<>();
    Timer.Sample sample = Timer.start(meterRegistry);
    future.thenRun(() -> sample.stop(bulkIngestTimer));

    try {
      byte[] bulkRequestBytes = bulkRequest.getBytes(StandardCharsets.UTF_8);
      incomingByteTotal.increment(bulkRequestBytes.length);
      Map<String, List<Trace.Span>> docs = BulkApiRequestParser.parseRequest(bulkRequestBytes);

      // todo - our rate limiter doesn't have a way to acquire permits across multiple
      // datasets
      // so today as a limitation we reject any request that has documents against
      // multiple indexes
      // We think most indexing requests will be against 1 index
      if (docs.keySet().size() > 1) {
        BulkIngestResponse response =
            new BulkIngestResponse(0, 0, "request must contain only 1 unique index");
        future.complete(HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response));
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
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  BulkIngestResponse response =
                      bulkIngestKafkaProducer.submitRequest(docs).getResponse();
                  future.complete(HttpResponse.ofJson(response));
                } catch (InterruptedException e) {
                  LOG.error("Request failed ", e);
                  future.complete(
                      HttpResponse.ofJson(
                          INTERNAL_SERVER_ERROR, new BulkIngestResponse(0, 0, e.getMessage())));
                }
              });
    } catch (Exception e) {
      LOG.error("Request failed ", e);
      BulkIngestResponse response = new BulkIngestResponse(0, 0, e.getMessage());
      future.complete(HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response));
    }

    return HttpResponse.of(future);
  }
}
