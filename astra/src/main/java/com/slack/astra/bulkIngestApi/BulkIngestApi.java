package com.slack.astra.bulkIngestApi;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.linecorp.armeria.common.HttpStatus.TOO_MANY_REQUESTS;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for defining the http endpoint behavior for the bulk ingest. It is
 * expected to handle appropriate rate limiting, error handling, and submit the parsed messages to
 * Kafka for ingestion.
 */
public class BulkIngestApi {
  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestApi.class);

  public static final String SCHEMA_ENFORCEMENT_FLAG = "astra.schema.enforcement.enabled";

  public static final String BULK_INGEST_INFLIGHT_BYTES = "astra_preprocessor_inflight_bytes";
  public static final String BULK_INGEST_INFLIGHT_REQUESTS = "astra_preprocessor_inflight_requests";
  public static final String BULK_INGEST_INFLIGHT_LIMIT_EXCEEDED =
      "astra_preprocessor_inflight_limit_exceeded";
  private static final long INFLIGHT_LIMIT_LOG_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);

  private final BulkIngestKafkaProducer bulkIngestKafkaProducer;
  private final DatasetRateLimitingService datasetRateLimitingService;
  private final Schema.IngestSchema schema;
  private final AstraConfigs.SchemaMode schemaMode;
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

  // Raw request bytes admitted but not yet completed. Tracked even when the cap is disabled so the
  // gauge can be used to size it.
  private final long maxInflightRequestBytes;
  private final boolean maxInflightRequestBytesDryRun;
  private final AtomicLong inflightBytes = new AtomicLong();
  private final AtomicInteger inflightRequests = new AtomicInteger();
  private final Counter inflightRejectedCounter;
  private final Counter inflightDryRunCounter;
  private final AtomicLong inflightExceededSinceLastLog = new AtomicLong();
  private final AtomicLong inflightLastLogMs = new AtomicLong();

  public BulkIngestApi(
      BulkIngestKafkaProducer bulkIngestKafkaProducer,
      DatasetRateLimitingService datasetRateLimitingService,
      MeterRegistry meterRegistry,
      int rateLimitExceededErrorCode,
      Schema.IngestSchema schema,
      AstraConfigs.SchemaMode schemaMode,
      long maxInflightRequestBytes,
      boolean maxInflightRequestBytesDryRun) {

    this.bulkIngestKafkaProducer = bulkIngestKafkaProducer;
    this.datasetRateLimitingService = datasetRateLimitingService;
    this.schema = schema;
    this.schemaMode = schemaMode;
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

    this.maxInflightRequestBytes = maxInflightRequestBytes;
    this.maxInflightRequestBytesDryRun = maxInflightRequestBytesDryRun;
    meterRegistry.gauge(BULK_INGEST_INFLIGHT_BYTES, inflightBytes);
    meterRegistry.gauge(BULK_INGEST_INFLIGHT_REQUESTS, inflightRequests);
    this.inflightRejectedCounter =
        meterRegistry.counter(BULK_INGEST_INFLIGHT_LIMIT_EXCEEDED, "action", "rejected");
    this.inflightDryRunCounter =
        meterRegistry.counter(BULK_INGEST_INFLIGHT_LIMIT_EXCEEDED, "action", "dry_run");
  }

  @Post("/_bulk")
  public HttpResponse addDocument(String bulkRequest) {
    // 1. Astra does not support the concept of "updates". It's always an add.
    // 2. The "index" is used as the span name
    CompletableFuture<HttpResponse> future = new CompletableFuture<>();
    Timer.Sample sample = Timer.start(meterRegistry);
    future.thenRun(() -> sample.stop(bulkIngestTimer));

    byte[] bulkRequestBytes = bulkRequest.getBytes(StandardCharsets.UTF_8);
    final long requestBytes = bulkRequestBytes.length;
    if (!tryAdmitInflight(requestBytes)) {
      BulkIngestResponse response = new BulkIngestResponse(0, 0, "too many in-flight bytes");
      future.complete(HttpResponse.ofJson(TOO_MANY_REQUESTS, response));
      return HttpResponse.of(future);
    }
    // Released when the work finishes, not when the client times out, as the docs stay in memory
    // until the producer is done with them
    future.whenComplete(
        (_, _) -> {
          inflightBytes.addAndGet(-requestBytes);
          inflightRequests.decrementAndGet();
        });

    try {
      incomingByteTotal.increment(bulkRequestBytes.length);
      Map<String, List<Trace.Span>> docs = Map.of();
      try {
        if (Boolean.getBoolean(SCHEMA_ENFORCEMENT_FLAG)) {
          docs = BulkApiRequestParser.parseRequest(bulkRequestBytes, schema, schemaMode);
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
                } catch (Exception e) {
                  // Any exception must still complete the future, or the client hangs until it
                  // times out and the in-flight bytes are never released
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
   * Reserves the request's bytes against the in-flight cap. Returns false if the request should be
   * rejected. Bytes are added optimistically and rolled back on rejection, so concurrent requests
   * near the cap may be rejected slightly early; that errs on the side of protecting the heap.
   */
  private boolean tryAdmitInflight(long requestBytes) {
    long inflight = inflightBytes.addAndGet(requestBytes);
    // A request larger than the whole cap is still admitted when nothing else is in flight, so it
    // can't be rejected forever
    if (maxInflightRequestBytes <= 0
        || inflight <= maxInflightRequestBytes
        || inflight == requestBytes) {
      inflightRequests.incrementAndGet();
      return true;
    }

    if (maxInflightRequestBytesDryRun) {
      inflightDryRunCounter.increment();
      logInflightLimitExceeded(inflight);
      inflightRequests.incrementAndGet();
      return true;
    }

    inflightBytes.addAndGet(-requestBytes);
    inflightRejectedCounter.increment();
    logInflightLimitExceeded(inflight);
    return false;
  }

  // Logs at most once per interval: this fires under memory pressure, where logging every request
  // would add to the problem
  private void logInflightLimitExceeded(long inflight) {
    inflightExceededSinceLastLog.incrementAndGet();
    long now = System.currentTimeMillis();
    long last = inflightLastLogMs.get();
    if (now - last < INFLIGHT_LIMIT_LOG_INTERVAL_MS
        || !inflightLastLogMs.compareAndSet(last, now)) {
      return;
    }
    LOG.warn(
        "{} {} bulk requests over the in-flight limit since last report: inflightBytes={},"
            + " maxInflightRequestBytes={}",
        maxInflightRequestBytesDryRun ? "Dry run, would have rejected" : "Rejected",
        inflightExceededSinceLastLog.getAndSet(0),
        inflight,
        maxInflightRequestBytes);
  }
}
