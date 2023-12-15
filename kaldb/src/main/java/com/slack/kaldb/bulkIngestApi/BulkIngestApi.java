package com.slack.kaldb.bulkIngestApi;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.linecorp.armeria.common.HttpStatus.TOO_MANY_REQUESTS;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.kaldb.preprocessor.ingest.OpenSearchBulkApiRequestParser;
import com.slack.service.murron.trace.Trace;
import java.util.List;
import java.util.Map;
import org.opensearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkIngestApi {
  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestApi.class);
  private final TransactionBatchingKafkaProducer transactionBatchingKafkaProducer;
  private final DatasetRateLimitingService datasetRateLimitingService;

  public BulkIngestApi(
      TransactionBatchingKafkaProducer transactionBatchingKafkaProducer,
      DatasetRateLimitingService datasetRateLimitingService) {

    this.transactionBatchingKafkaProducer = transactionBatchingKafkaProducer;
    this.datasetRateLimitingService = datasetRateLimitingService;
  }

  /**
   * 1. Kaldb does not support the concept of "updates". It's always an add 2. The "index" is used
   * as the span name
   */
  @Blocking
  @Post("/_bulk")
  public HttpResponse addDocument(String bulkRequest) {
    try {
      List<IndexRequest> indexRequests =
          OpenSearchBulkApiRequestParser.parseBulkRequest(bulkRequest);
      Map<String, List<Trace.Span>> docs =
          OpenSearchBulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);

      // todo - our rate limiter doesn't have a way to acquire permits across multiple datasets
      // so today as a limitation we reject any request that has documents against multiple indexes
      // We think most indexing requests will be against 1 index
      if (docs.keySet().size() > 1) {
        BulkIngestResponse response =
            new BulkIngestResponse(0, 0, "request must contain only 1 unique index");
        return HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response);
      }

      for (Map.Entry<String, List<Trace.Span>> indexDocs : docs.entrySet()) {
        final String index = indexDocs.getKey();
        if (!datasetRateLimitingService.test(index, indexDocs.getValue())) {
          BulkIngestResponse response = new BulkIngestResponse(0, 0, "rate limit exceeded");
          return HttpResponse.ofJson(TOO_MANY_REQUESTS, response);
        }
      }
      BulkIngestResponse response =
          transactionBatchingKafkaProducer.createRequest(docs).getResponse();
      return HttpResponse.ofJson(response);
    } catch (Exception e) {
      LOG.error("Request failed ", e);
      BulkIngestResponse response = new BulkIngestResponse(0, 0, e.getMessage());
      return HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response);
    }
  }
}
