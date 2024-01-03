package com.slack.kaldb.bulkIngestApi;

import static com.google.common.base.Preconditions.checkArgument;
import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_ALL_SERVICE;
import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_STAR_SERVICE;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.preprocessor.PreprocessorService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.util.RuntimeHalterImpl;
import com.slack.kaldb.writer.KafkaUtils;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkIngestKafkaProducer extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestKafkaProducer.class);

  private final KafkaProducer<String, byte[]> kafkaProducer;
  private final KafkaClientMetrics kafkaMetrics;

  private final KaldbConfigs.KafkaConfig kafkaConfig;

  private final DatasetMetadataStore datasetMetadataStore;
  private final KaldbMetadataStoreChangeListener<DatasetMetadata> datasetListener =
      (_) -> cacheSortedDataset();

  protected List<DatasetMetadata> throughputSortedDatasets;

  private final BlockingQueue<BulkIngestRequest> pendingRequests;

  private final Integer producerSleep;

  public static final String FAILED_SET_RESPONSE_COUNTER =
      "bulk_ingest_producer_failed_set_response";
  private final Counter failedSetResponseCounter;

  public static final String OVER_LIMIT_PENDING_REQUESTS =
      "bulk_ingest_producer_over_limit_pending";
  private final Counter overLimitPendingRequests;

  public static final String STALL_COUNTER = "bulk_ingest_producer_stall_counter";
  private final Counter stallCounter;

  public static final String BATCH_SIZE_GAUGE = "bulk_ingest_producer_batch_size";
  private final AtomicInteger batchSizeGauge;

  private static final Set<String> OVERRIDABLE_CONFIGS =
      Set.of(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

  public BulkIngestKafkaProducer(
      final DatasetMetadataStore datasetMetadataStore,
      final KaldbConfigs.PreprocessorConfig preprocessorConfig,
      final PrometheusMeterRegistry meterRegistry) {

    this.kafkaConfig = preprocessorConfig.getKafkaConfig();

    checkArgument(
        !kafkaConfig.getKafkaBootStrapServers().isEmpty(),
        "Kafka bootstrapServers must be provided");

    checkArgument(!kafkaConfig.getKafkaTopic().isEmpty(), "Kafka topic must be provided");

    this.datasetMetadataStore = datasetMetadataStore;

    // todo - consider making these a configurable value, or determine a way to derive a reasonable
    // value automatically
    this.pendingRequests =
        new ArrayBlockingQueue<>(
            Integer.parseInt(System.getProperty("kalDb.bulkIngest.pendingLimit", "500")));
    this.producerSleep =
        Integer.parseInt(System.getProperty("kalDb.bulkIngest.producerSleep", "2000"));

    // since we use a new transaction ID every time we start a preprocessor there can be some zombie
    // transactions?
    // I think they will remain in kafka till they expire. They should never be readable if the
    // consumer sets isolation.level as "read_committed"
    // see "zombie fencing" https://www.confluent.io/blog/transactions-apache-kafka/
    this.kafkaProducer = createKafkaTransactionProducer(UUID.randomUUID().toString());

    this.kafkaMetrics = new KafkaClientMetrics(kafkaProducer);
    this.kafkaMetrics.bindTo(meterRegistry);

    this.failedSetResponseCounter = meterRegistry.counter(FAILED_SET_RESPONSE_COUNTER);
    this.overLimitPendingRequests = meterRegistry.counter(OVER_LIMIT_PENDING_REQUESTS);
    this.stallCounter = meterRegistry.counter(STALL_COUNTER);
    this.batchSizeGauge = meterRegistry.gauge(BATCH_SIZE_GAUGE, new AtomicInteger(0));

    this.kafkaProducer.initTransactions();
  }

  private void cacheSortedDataset() {
    // we sort the datasets to rank from which dataset do we start matching candidate service names
    // in the future we can change the ordering from sort to something else
    this.throughputSortedDatasets =
        datasetMetadataStore.listSync().stream()
            .sorted(Comparator.comparingLong(DatasetMetadata::getThroughputBytes).reversed())
            .toList();
  }

  @Override
  protected void startUp() throws Exception {
    cacheSortedDataset();
    datasetMetadataStore.addListener(datasetListener);
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      List<BulkIngestRequest> requests = new ArrayList<>();
      pendingRequests.drainTo(requests);
      batchSizeGauge.set(requests.size());
      if (requests.isEmpty()) {
        try {
          stallCounter.increment();
          Thread.sleep(producerSleep);
        } catch (InterruptedException e) {
          return;
        }
      } else {
        transactionCommit(requests);
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    datasetMetadataStore.removeListener(datasetListener);

    kafkaProducer.close();
    if (kafkaMetrics != null) {
      kafkaMetrics.close();
    }
  }

  public BulkIngestRequest submitRequest(Map<String, List<Trace.Span>> inputDocs) {
    BulkIngestRequest request = new BulkIngestRequest(inputDocs);
    try {
      pendingRequests.add(request);
    } catch (IllegalStateException e) {
      overLimitPendingRequests.increment();
      throw e;
    }
    return request;
  }

  protected Map<BulkIngestRequest, BulkIngestResponse> transactionCommit(
      List<BulkIngestRequest> requests) {
    Map<BulkIngestRequest, BulkIngestResponse> responseMap = new HashMap<>();
    try {
      kafkaProducer.beginTransaction();
      for (BulkIngestRequest request : requests) {
        responseMap.put(request, produceDocuments(request.getInputDocs()));
      }
      kafkaProducer.commitTransaction();
    } catch (TimeoutException te) {
      LOG.error("Commit transaction timeout", te);
      // the commitTransaction waits till "max.block.ms" after which it will time out
      // in that case we cannot call abort exception because that throws the following error
      // "Cannot attempt operation `abortTransaction` because the previous
      // call to `commitTransaction` timed out and must be retried"
      // so for now we just restart the preprocessor
      new RuntimeHalterImpl()
          .handleFatal(
              new Throwable(
                  "KafkaProducer needs to shutdown as we don't have retry yet and we cannot call abortTxn on timeout",
                  te));
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

      for (BulkIngestRequest request : requests) {
        responseMap.put(
            request,
            new BulkIngestResponse(
                0,
                request.getInputDocs().values().stream().mapToInt(List::size).sum(),
                e.getMessage()));
      }
    }

    for (Map.Entry<BulkIngestRequest, BulkIngestResponse> entry : responseMap.entrySet()) {
      BulkIngestRequest key = entry.getKey();
      BulkIngestResponse value = entry.getValue();
      if (!key.setResponse(value)) {
        LOG.warn("Failed to add result to the bulk ingest request, consumer thread went away?");
        failedSetResponseCounter.increment();
      }
    }
    return responseMap;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private BulkIngestResponse produceDocuments(Map<String, List<Trace.Span>> indexDocs) {
    int totalDocs = indexDocs.values().stream().mapToInt(List::size).sum();

    // we cannot create a generic pool of producers because the kafka API expects the transaction ID
    // to be a property while creating the producer object.
    for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
      String index = indexDoc.getKey();

      // call once per batch and use the same partition for better batching
      // todo - this probably shouldn't be tied to the transaction batching logic?
      int partition = getPartition(index);

      // since there isn't a dataset provisioned for this service/index we will not index this set
      // of docs
      if (partition < 0) {
        LOG.warn("index=" + index + " does not have a provisioned dataset associated with it");
        continue;
      }

      // KafkaProducer does not allow creating multiple transactions from a single object -
      // rightfully so.
      // Till we fix the producer design to allow for multiple /_bulk requests to be able to
      // write to the same txn
      // we will limit producing documents 1 thread at a time
      for (Trace.Span doc : indexDoc.getValue()) {
        ProducerRecord<String, byte[]> producerRecord =
            new ProducerRecord<>(kafkaConfig.getKafkaTopic(), partition, index, doc.toByteArray());

        // we intentionally supress FutureReturnValueIgnored here in errorprone - this is because
        // we wrap this in a transaction, which is responsible for flushing all of the pending
        // messages
        kafkaProducer.send(producerRecord);
      }
    }

    return new BulkIngestResponse(totalDocs, 0, "");
  }

  private KafkaProducer<String, byte[]> createKafkaTransactionProducer(String transactionId) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaBootStrapServers());
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

    // don't override the properties that we have already set explicitly using named properties
    for (Map.Entry<String, String> additionalProp :
        kafkaConfig.getAdditionalPropsMap().entrySet()) {
      props =
          KafkaUtils.maybeOverrideProps(
              props,
              additionalProp.getKey(),
              additionalProp.getValue(),
              OVERRIDABLE_CONFIGS.contains(additionalProp.getKey()));
    }
    return new KafkaProducer<>(props);
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
}
