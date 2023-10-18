package com.slack.kaldb.server;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.kaldb.elasticsearchApi.BulkIngestResponse;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.preprocessor.PreprocessorRateLimiter;
import com.slack.kaldb.preprocessor.PreprocessorService;
import com.slack.kaldb.preprocessor.ingest.OpenSearchBulkApiRequestParser;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.util.RuntimeHalterImpl;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiPredicate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * batching is important - if we send one doc a time we will create a transaction per request which
 * is expensive
 */
public class OpenSearchBulkIngestApi extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchBulkIngestApi.class);
  private final PrometheusMeterRegistry meterRegistry;

  private final KaldbConfigs.PreprocessorConfig preprocessorConfig;
  private final DatasetMetadataStore datasetMetadataStore;

  private final KaldbMetadataStoreChangeListener<DatasetMetadata> datasetListener =
      (datasetMetadata) -> load();

  private final PreprocessorRateLimiter rateLimiter;
  private BiPredicate<String, List<Trace.Span>> rateLimiterPredicate;
  private List<DatasetMetadata> throughputSortedDatasets;

  private final Timer configReloadTimer;

  private final KafkaProducer kafkaProducer;

  @Override
  protected void doStart() {
    try {
      LOG.info("Starting OpenSearchBulkIngestApi service");
      load();
      datasetMetadataStore.addListener(datasetListener);
      LOG.info("OpenSearchBulkIngestAPI service started");
      notifyStarted();
    } catch (Throwable t) {
      notifyFailed(t);
    }
  }

  @Override
  protected void doStop() {
    try {
      LOG.info("Stopping OpenSearchBulkIngestApi service");
      datasetMetadataStore.removeListener(datasetListener);
      kafkaProducer.close();
      LOG.info("OpenSearchBulkIngestApi service closed");
      notifyStopped();
    } catch (Throwable t) {
      notifyFailed(t);
    }
  }

  public void load() {
    Timer.Sample sample = Timer.start(meterRegistry);
    try {
      List<DatasetMetadata> datasetMetadataList = datasetMetadataStore.listSync();
      // only attempt to register stream processing on valid dataset configurations
      List<DatasetMetadata> datasetMetadataToProcesses =
          filterValidDatasetMetadata(datasetMetadataList);

      checkState(!datasetMetadataToProcesses.isEmpty(), "dataset metadata list must not be empty");

      this.throughputSortedDatasets = sortDatasetsOnThroughput(datasetMetadataToProcesses);
      this.rateLimiterPredicate =
          rateLimiter.createBulkIngestRateLimiter(datasetMetadataToProcesses);
    } catch (Exception e) {
      notifyFailed(e);
    } finally {
      // TODO: re-work this so that we can add success/failure tags and capture them
      sample.stop(configReloadTimer);
    }
  }

  public OpenSearchBulkIngestApi(
      DatasetMetadataStore datasetMetadataStore,
      KaldbConfigs.PreprocessorConfig preprocessorConfig,
      PrometheusMeterRegistry meterRegistry) {
    this(datasetMetadataStore, preprocessorConfig, meterRegistry, INITIALIZE_RATE_LIMIT_WARM);
  }

  public OpenSearchBulkIngestApi(
      DatasetMetadataStore datasetMetadataStore,
      KaldbConfigs.PreprocessorConfig preprocessorConfig,
      PrometheusMeterRegistry meterRegistry,
      boolean initializeRateLimitWarm) {

    checkArgument(
        !preprocessorConfig.getBootstrapServers().isEmpty(),
        "Kafka bootstrapServers must be provided");

    checkArgument(
        !Strings.isEmpty(preprocessorConfig.getDownstreamTopic()),
        "Kafka downstreamTopic must be provided");

    this.datasetMetadataStore = datasetMetadataStore;
    this.preprocessorConfig = preprocessorConfig;
    this.meterRegistry = meterRegistry;
    this.rateLimiter =
        new PreprocessorRateLimiter(
            meterRegistry,
            preprocessorConfig.getPreprocessorInstanceCount(),
            preprocessorConfig.getRateLimiterMaxBurstSeconds(),
            initializeRateLimitWarm);

    this.configReloadTimer = meterRegistry.timer(CONFIG_RELOAD_TIMER);
    // since we use a new transaction ID every time we start a preprocessor there can be some zombie
    // transactions?
    // I think they will remain in kafka till they expire. They should never be readable if the
    // consumer sets isolation.level as "read_committed"
    // see "zombie fencing" https://www.confluent.io/blog/transactions-apache-kafka/
    this.kafkaProducer = createKafkaTransactionProducer(UUID.randomUUID().toString());
    LOG.info("Calling initTransactions");
    this.kafkaProducer.initTransactions();
  }

  // along with the bulk API we also need to expose some node info that logstash needs info from
  @Get("/")
  public HttpResponse getNodeInfo() {
    String output =
        """
                    {
                      "name" : "node_name",
                      "cluster_name" : "cluster_name",
                      "cluster_uuid" : "uuid",
                      "version" : {
                        "number" : "7.12.0",
                        "build_flavor" : "default",
                        "build_type" : "deb",
                        "build_hash" : "78722783c38caa25a70982b5b042074cde5d3b3a",
                        "build_date" : "2021-04-02T00:53:29.130908562Z",
                        "build_snapshot" : false,
                        "lucene_version" : "8.8.0",
                        "minimum_wire_compatibility_version" : "6.8.0",
                        "minimum_index_compatibility_version" : "6.0.0-beta1"
                      },
                      "tagline" : "You Know, for Search"
                    }
                    """;
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Get("/_license")
  public HttpResponse getLicenseInfo() {
    String output =
        """
                        {
                          "license" : {
                            "status" : "active",
                            "uid" : "8afdc262-f37a-4b48-ad2e-68e224180640",
                            "type" : "basic",
                            "issue_date" : "2020-12-07T23:59:22.009Z",
                            "issue_date_in_millis" : 1607385562009,
                            "max_nodes" : 1000,
                            "issued_to" : "cluster_name",
                            "issuer" : "elasticsearch",
                            "start_date_in_millis" : -1
                          }
                        }
                        """;
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
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
      // our rate limiter doesn't have a way to acquire permits across multiple datasets
      // so today as a limitation we reject any request that has documents against multiple indexes
      // We think most indexing requests will be against 1 index
      if (docs.keySet().size() > 1) {
        BulkIngestResponse response =
            new BulkIngestResponse(0, 0, "request must contain only 1 unique index");
        return HttpResponse.ofJson(INTERNAL_SERVER_ERROR, response);
      }

      for (Map.Entry<String, List<Trace.Span>> indexDocs : docs.entrySet()) {
        final String index = indexDocs.getKey();
        if (!rateLimiterPredicate.test(index, indexDocs.getValue())) {
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

  public synchronized BulkIngestResponse produceDocuments(Map<String, List<Trace.Span>> indexDocs) {
    int totalDocs = indexDocs.values().stream().mapToInt(List::size).sum();

    // we cannot create a generic pool of producers because the kafka API expects the transaction ID
    // to be a property while creating the producer object.
    for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
      String index = indexDoc.getKey();
      // call once per batch and use the same partition for better batching
      int partition = getPartition(index);

      // since there isn't a dataset provisioned for this service/index we will not index this set
      // of docs
      if (partition < 0) {
        LOG.warn("index=" + index + " does not have a provisioned dataset associated with it");
        continue;
      }
      try {
        kafkaProducer.beginTransaction();
        for (Trace.Span doc : indexDoc.getValue()) {

          ProducerRecord<String, byte[]> producerRecord =
              new ProducerRecord<>(
                  preprocessorConfig.getDownstreamTopic(), partition, index, doc.toByteArray());
          kafkaProducer.send(producerRecord);
        }
        kafkaProducer.commitTransaction();
      } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
        // We can't recover from these exceptions, so our only option is to close the producer and
        // exit.
        new RuntimeHalterImpl().handleFatal(new Throwable("KafkaProducer needs to shutdown ", e));
      } catch (Exception e) {
        LOG.warn("failed transaction with error", e);
        try {
          kafkaProducer.abortTransaction();
        } catch (ProducerFencedException err) {
          LOG.error("Could not abort transaction", err);
        }
        return new BulkIngestResponse(0, totalDocs, e.getMessage());
      }
    }

    return new BulkIngestResponse(totalDocs, 0, "");
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

  private KafkaProducer<String, byte[]> createKafkaTransactionProducer(String transactionId) {
    Properties props = new Properties();
    props.put("bootstrap.servers", preprocessorConfig.getBootstrapServers());
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("transactional.id", transactionId);
    props.put("linger.ms", 250);
    return new KafkaProducer<>(props);
  }
}
