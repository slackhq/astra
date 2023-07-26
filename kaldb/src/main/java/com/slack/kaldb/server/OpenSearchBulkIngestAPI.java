package com.slack.kaldb.server;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.linecorp.armeria.common.HttpStatus.TOO_MANY_REQUESTS;
import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_ALL_SERVICE;
import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_STAR_SERVICE;
import static com.slack.kaldb.preprocessor.PreprocessorService.CONFIG_RELOAD_TIMER;
import static com.slack.kaldb.preprocessor.PreprocessorService.INITIALIZE_RATE_LIMIT_WARM;
import static com.slack.kaldb.preprocessor.PreprocessorService.filterValidDatasetMetadata;
import static com.slack.kaldb.preprocessor.PreprocessorService.sortDatasetsOnThroughput;

import com.google.common.util.concurrent.AbstractService;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.kaldb.elasticsearchApi.BulkIngestResponse;
import com.slack.kaldb.elasticsearchApi.OpenSearchRequest;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.preprocessor.PreprocessorRateLimiter;
import com.slack.kaldb.preprocessor.PreprocessorService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.kstream.Predicate;
import org.opensearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchBulkIngestAPI extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchBulkIngestAPI.class);
  private final PrometheusMeterRegistry meterRegistry;

  private final KaldbConfigs.PreprocessorConfig preprocessorConfig;
  private final DatasetMetadataStore datasetMetadataStore;
  private final KafkaProducer<String, byte[]> kafkaProducer;

  private final KaldbMetadataStoreChangeListener<DatasetMetadata> datasetListener =
      (datasetMetadata) -> load();

  private final PreprocessorRateLimiter rateLimiter;
  private Predicate<String, List<Trace.Span>> rateLimiterPredicate;
  private List<DatasetMetadata> throughputSortedDatasets;

  private final Timer configReloadTimer;

  @Override
  protected void doStart() {
    try {
      startUp();
      notifyStarted();
    } catch (Throwable t) {
      notifyFailed(t);
    }
  }

  @Override
  protected void doStop() {
    try {
      shutDown();
      notifyStopped();
    } catch (Throwable t) {
      notifyFailed(t);
    }
  }

  private void startUp() {
    LOG.info("Starting OpenSearchBulkIngestAPI service");
    load();
    datasetMetadataStore.addListener(datasetListener);
    LOG.info("OpenSearchBulkIngestAPI service started");
  }

  private void shutDown() {
    LOG.info("Stopping OpenSearchBulkIngestAPI service");
    datasetMetadataStore.removeListener(datasetListener);
    kafkaProducer.close();
    LOG.info("OpenSearchBulkIngestAPI service closed");
  }

  public void load() {
    Timer.Sample loadTimer = null;
    try {
      loadTimer = Timer.start(meterRegistry);
      List<DatasetMetadata> datasetMetadataList = datasetMetadataStore.listSync();
      // only attempt to register stream processing on valid dataset configurations
      List<DatasetMetadata> datasetMetadataToProcesses =
          filterValidDatasetMetadata(datasetMetadataList);

      this.throughputSortedDatasets = sortDatasetsOnThroughput(datasetMetadataToProcesses);
      this.rateLimiterPredicate =
          rateLimiter.createBulkIngestRateLimiter(datasetMetadataToProcesses);

    } catch (Exception e) {
      notifyFailed(e);
    } finally {
      if (loadTimer != null) {
        loadTimer.stop(configReloadTimer);
      }
    }
  }

  public OpenSearchBulkIngestAPI(
      DatasetMetadataStore datasetMetadataStore,
      KaldbConfigs.PreprocessorConfig preprocessorConfig,
      PrometheusMeterRegistry meterRegistry) {
    this.datasetMetadataStore = datasetMetadataStore;
    this.preprocessorConfig = preprocessorConfig;
    this.meterRegistry = meterRegistry;
    this.kafkaProducer = createKafkaProducer();
    this.rateLimiter =
        new PreprocessorRateLimiter(
            meterRegistry,
            preprocessorConfig.getPreprocessorInstanceCount(),
            preprocessorConfig.getRateLimiterMaxBurstSeconds(),
            INITIALIZE_RATE_LIMIT_WARM);

    this.configReloadTimer = meterRegistry.timer(CONFIG_RELOAD_TIMER);
  }

  /**
   * 1. Kaldb does not support the concept of "updates". It's always an add 2. The "index" is used
   * as the span name
   */
  @Blocking
  @Post("/_bulk")
  public HttpResponse addDocument(String bulkRequest) {
    try {
      List<IndexRequest> indexRequests = OpenSearchRequest.parseBulkRequest(bulkRequest);
      Map<String, List<Trace.Span>> docs =
          OpenSearchRequest.convertIndexRequestToTraceFormat(indexRequests);
      // our rate limiter doesn't have a way to acquire permits across multiple datasets
      // so today as a limitation we reject any request that has documents against multiple indexes
      // We think most indexing requests will be against 1 index
      if (docs.keySet().size() > 1) {
        BulkIngestResponse response =
            new BulkIngestResponse(0, 0, "request must contain only 1 index");
        return HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response);
      }

      for (Map.Entry<String, List<Trace.Span>> indexDoc : docs.entrySet()) {
        final String index = indexDoc.getKey();
        if (!rateLimiterPredicate.test(index, indexDoc.getValue())) {
          BulkIngestResponse response = new BulkIngestResponse(0, 0, "rate limit exceeded");
          return HttpResponse.ofJson(TOO_MANY_REQUESTS, response);
        }
      }
      BulkIngestResponse response = produceDocuments(docs);
      return HttpResponse.ofJson(response);
    } catch (Exception e) {
      LOG.error("Request failed ", e);
      BulkIngestResponse response = new BulkIngestResponse(0, 0, e.getMessage());
      return HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response);
    }
  }

  public BulkIngestResponse produceDocuments(Map<String, List<Trace.Span>> indexDocs) {
    int totalDocs = indexDocs.values().stream().mapToInt(List::size).sum();

    final CountDownLatch completeSignal = new CountDownLatch(totalDocs);
    AtomicLong errorCount = new AtomicLong();
    for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
      String index = indexDoc.getKey();
      int partition = getPartition(index);

      // since there isn't a dataset provisioned for this service/index we will not index this set
      // of docs
      if (partition < 0) {
        LOG.warn("index=" + index + " does not have a provisioned dataset associated with it");
        continue;
      }
      for (Trace.Span doc : indexDoc.getValue()) {
        KafkaBlockingCallback producerReturnInfo =
            new KafkaBlockingCallback(completeSignal, errorCount);

        ProducerRecord<String, byte[]> producerRecord =
            new ProducerRecord<>(
                preprocessorConfig.getDownstreamTopic(), partition, index, doc.toByteArray());
        kafkaProducer.send(producerRecord, producerReturnInfo);
      }
      kafkaProducer.flush();
      try {
        completeSignal.await();
      } catch (InterruptedException e) {
        LOG.error("Failed while waiting for all documents to be ACK'ed ", e);
      }
    }
    return new BulkIngestResponse(totalDocs, errorCount.get(), "");
  }

  private int getPartition(String index) {
    for (DatasetMetadata datasetMetadata : throughputSortedDatasets) {
      String serviceNamePattern = datasetMetadata.getServiceNamePattern();

      if (serviceNamePattern.equals(MATCH_ALL_SERVICE)
          || serviceNamePattern.equals(MATCH_STAR_SERVICE)
          || index.equals(serviceNamePattern)) {
        List<Integer> partitions = PreprocessorService.getActivePartitionList(datasetMetadata);
        return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
      }
    }
    // We don't have a provisioned service for this index
    return -1;
  }

  class KafkaBlockingCallback implements Callback {

    private final CountDownLatch complete;
    private final AtomicLong errorCount;
    String exceptionMessage = null;

    public KafkaBlockingCallback(CountDownLatch complete, AtomicLong errorCount) {
      this.errorCount = errorCount;
      this.complete = complete;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
      complete.countDown();
      if (e != null) {
        errorCount.incrementAndGet();
        if (exceptionMessage != null) {
          LOG.error("Failed to write to kafka ", e);
        }
      }
    }
  }

  private KafkaProducer<String, byte[]> createKafkaProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", preprocessorConfig.getKafkaStreamConfig().getBootstrapServers());
    props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    // from the kafka producer javadocs
    // Note that records that arrive close together in time will generally batch together even with
    // <code>linger.ms=0</code>
    props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
    return new KafkaProducer<>(props);
  }
}
