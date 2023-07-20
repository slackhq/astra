package com.slack.kaldb.server;

import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_ALL_SERVICE;
import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_STAR_SERVICE;
import static com.slack.kaldb.preprocessor.PreprocessorService.filterValidDatasetMetadata;
import static com.slack.kaldb.preprocessor.PreprocessorService.sortDatasetsOnThroughput;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.kaldb.elasticsearchApi.OpenSearchRequest;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.preprocessor.PreprocessorService;
import com.slack.kaldb.preprocessor.PreprocessorValueMapper;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.service.murron.trace.Trace;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchBulkIngestAPI {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchBulkIngestAPI.class);

  private final KaldbConfigs.PreprocessorConfig preprocessorConfig;
  private final PrometheusMeterRegistry meterRegistry;
  private final DatasetMetadataStore datasetMetadataStore;
  private final KafkaProducer<String, byte[]> kafkaProducer;

  final List<DatasetMetadata> throughputSortedDatasets;

  public ElasticsearchBulkIngestAPI(
      DatasetMetadataStore datasetMetadataStore,
      KaldbConfigs.PreprocessorConfig preprocessorConfig,
      PrometheusMeterRegistry meterRegistry) {
    this.datasetMetadataStore = datasetMetadataStore;
    this.preprocessorConfig = preprocessorConfig;
    this.meterRegistry = meterRegistry;
    this.kafkaProducer = createKafkaProducer();

    // only attempt to register stream processing on valid dataset configurations
    List<DatasetMetadata> datasetMetadataList = datasetMetadataStore.listSync();
    List<DatasetMetadata> datasetMetadataToProcesses =
        filterValidDatasetMetadata(datasetMetadataList);

    this.throughputSortedDatasets = sortDatasetsOnThroughput(datasetMetadataToProcesses);
  }

  /**
   * 1. Kaldb does not support the concept of "updates". It's always an add 2. The "index" is used
   * as the span name
   */
  @Blocking
  @Post("/_bulk")
  public HttpResponse addDocument(String bulkRequest) {

    Map<String, List<Trace.Span>> docs = OpenSearchRequest.parseBulkHttpRequest(bulkRequest);
    // TODO: Add rate limit
    produceDocuments(docs);
    return HttpResponse.ofJson(bulkRequest);
  }

  public void produceDocuments(Map<String, List<Trace.Span>> indexDocs) {
    int totalDocs = indexDocs.values().stream().mapToInt(List::size).sum();

    final CountDownLatch completeSignal = new CountDownLatch(totalDocs);
    AtomicLong errorCount = new AtomicLong();
    for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
      String index = indexDoc.getKey();
      int partition = getPartition(index);

      if (partition < 0) {
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
        LOG.error("Error while waiting for all docs to be ACK'ed by kafka", e);
      }
    }
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

  public int getPartition(Trace.Span span) {
    String serviceName = PreprocessorValueMapper.getServiceName(span);
    if (serviceName == null) {
      // this also should not happen since we drop messages with empty service names in the rate
      // limiter
      throw new IllegalStateException(
          String.format("Service name not found within the message '%s'", span));
    }
    return 1;
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
