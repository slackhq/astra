package com.slack.kaldb.preprocessor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.writer.kafka.KaldbKafkaConsumer.maybeOverride;

import com.google.common.util.concurrent.AbstractService;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PreprocessorService consumes from multiple upstream topics, applies rate limiting, transforms
 * the data format, and then writes out the new message to a common downstream topic targeting
 * specific partitions. The rate limits, and target partitions are read in via the
 * DatasetMetadataStore, with the upstream topic, downstream topic, and transforms stored in the
 * dataset config.
 *
 * <p>Changes to the DatasetMetadata will cause the existing Kafka Stream topology to be closed, and
 * this service will restart consumption with a new stream topology representing the newly updated
 * metadata. This class implements a doStart/doStop similar to the AbstractIdleService, but by
 * extending an AbstractService we also gain access to notifyFailed() in the event a subsequent load
 * were to fail.
 */
public class PreprocessorService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(PreprocessorService.class);
  private static final long MAX_TIME = Long.MAX_VALUE;

  private static final boolean INITIALIZE_RATE_LIMIT_WARM = true;

  private final DatasetMetadataStore datasetMetadataStore;
  private final PreprocessorRateLimiter rateLimiter;
  private final Properties kafkaProperties;
  private final List<String> upstreamTopics;
  private final String downstreamTopic;
  private final String dataTransformer;
  private final MeterRegistry meterRegistry;
  private final int kafkaPartitionStickyTimeoutMs;

  private KafkaStreams kafkaStreams;
  private KafkaStreamsMetrics kafkaStreamsMetrics;

  private final Timer configReloadTimer;
  public static final String CONFIG_RELOAD_TIMER = "preprocessor_config_reload_timer";

  public PreprocessorService(
      DatasetMetadataStore datasetMetadataStore,
      KaldbConfigs.PreprocessorConfig preprocessorConfig,
      MeterRegistry meterRegistry) {
    this.datasetMetadataStore = datasetMetadataStore;
    this.meterRegistry = meterRegistry;
    this.configReloadTimer = meterRegistry.timer(CONFIG_RELOAD_TIMER);

    this.kafkaProperties = makeKafkaStreamsProps(preprocessorConfig.getKafkaStreamConfig());
    this.downstreamTopic = preprocessorConfig.getDownstreamTopic();
    this.upstreamTopics = Collections.unmodifiableList(preprocessorConfig.getUpstreamTopicsList());
    this.dataTransformer = preprocessorConfig.getDataTransformer();
    this.rateLimiter =
        new PreprocessorRateLimiter(
            meterRegistry,
            preprocessorConfig.getPreprocessorInstanceCount(),
            preprocessorConfig.getRateLimiterMaxBurstSeconds(),
            INITIALIZE_RATE_LIMIT_WARM);
    this.kafkaPartitionStickyTimeoutMs = preprocessorConfig.getKafkaPartitionStickyTimeoutMs();
  }

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
    LOG.info("Starting preprocessor service");
    load();
    datasetMetadataStore.addListener(this::load);
    LOG.info("Preprocessor service started");
  }

  private void shutDown() {
    LOG.info("Stopping preprocessor service");
    if (kafkaStreams != null) {
      kafkaStreams.close(DEFAULT_START_STOP_DURATION);
    }
    if (kafkaStreamsMetrics != null) {
      kafkaStreamsMetrics.close();
    }
    datasetMetadataStore.removeListener(this::load);
    LOG.info("Preprocessor service closed");
  }

  /**
   * Configures and starts a KafkaStream processor, based off of the cached DatasetMetadataStore.
   * This method is reentrant, and will restart any existing KafkaStream processors. Access to this
   * must be synchronized if using this method as part of a listener.
   */
  public synchronized void load() {
    try {
      Timer.Sample loadTimer = Timer.start(meterRegistry);
      LOG.info("Loading new Kafka stream processor config");
      if (kafkaStreams != null) {
        LOG.info("Closing existing Kafka stream processor");
        kafkaStreams.close();
        kafkaStreams.cleanUp();
      }
      if (kafkaStreamsMetrics != null) {
        kafkaStreamsMetrics.close();
      }

      // only attempt to register stream processing on valid dataset configurations
      List<DatasetMetadata> datasetMetadataToProcesses =
          filterValidDatasetMetadata(datasetMetadataStore.listSync());

      if (datasetMetadataToProcesses.size() > 0) {
        Topology topology =
            buildTopology(
                datasetMetadataToProcesses,
                rateLimiter,
                upstreamTopics,
                downstreamTopic,
                dataTransformer,
                kafkaPartitionStickyTimeoutMs,
                meterRegistry);
        kafkaStreams = new KafkaStreams(topology, kafkaProperties);
        kafkaStreamsMetrics = new KafkaStreamsMetrics(kafkaStreams);
        kafkaStreamsMetrics.bindTo(meterRegistry);
        kafkaStreams.start();
        LOG.info("Kafka stream processor config loaded successfully");
      } else {
        LOG.info(
            "No valid dataset configurations found to process - will retry on next dataset configuration update");
      }
      loadTimer.stop(configReloadTimer);
    } catch (Exception e) {
      notifyFailed(e);
    }
  }

  /**
   * Builds a KafkaStream Topology with multiple source topics, targeting a single sink. Applies
   * rate limits per-dataset if required, and uses the dataset configured target topic sink and data
   * transformer.
   */
  protected static Topology buildTopology(
      List<DatasetMetadata> datasetMetadataList,
      PreprocessorRateLimiter rateLimiter,
      List<String> upstreamTopics,
      String downstreamTopic,
      String dataTransformer,
      int kafkaPartitionStickyTimeoutMs,
      MeterRegistry meterRegistry) {
    checkArgument(!datasetMetadataList.isEmpty(), "dataset metadata list must not be empty");
    checkArgument(upstreamTopics.size() > 0, "upstream topic list must not be empty");
    checkArgument(!downstreamTopic.isEmpty(), "downstream topic must not be empty");
    checkArgument(!dataTransformer.isEmpty(), "data transformer must not be empty");
    checkArgument(kafkaPartitionStickyTimeoutMs >= 0, "kafkaPartitionStickyTimeoutMs must be >=0");

    StreamsBuilder builder = new StreamsBuilder();

    ValueMapper<byte[], Iterable<Trace.Span>> valueMapper =
        PreprocessorValueMapper.byteArrayToTraceSpans(dataTransformer, meterRegistry);

    StreamPartitioner<String, Trace.Span> streamPartitioner =
        new PreprocessorPartitioner<>(datasetMetadataList, kafkaPartitionStickyTimeoutMs);

    Predicate<String, Trace.Span> rateLimitPredicate =
        rateLimiter.createRateLimiter(datasetMetadataList);

    upstreamTopics.forEach(
        (upstreamTopic ->
            builder
                .stream(upstreamTopic, Consumed.with(Serdes.String(), Serdes.ByteArray()))
                .flatMapValues(valueMapper)
                .filter(rateLimitPredicate)
                .peek(
                    (key, span) ->
                        LOG.debug(
                            "Processed span {} from topic {} to topic {}",
                            span,
                            upstreamTopic,
                            downstreamTopic))
                .to(
                    downstreamTopic,
                    Produced.with(Serdes.String(), KaldbSerdes.TraceSpan(), streamPartitioner))));

    return builder.build();
  }

  /**
   * Filters the provided list of dataset metadata to those that are valid. This includes correctly
   * defined throughput and partition configurations.
   */
  protected static List<DatasetMetadata> filterValidDatasetMetadata(
      List<DatasetMetadata> datasetMetadataList) {
    return datasetMetadataList
        .stream()
        .filter(datasetMetadata -> datasetMetadata.getThroughputBytes() > 0)
        .filter(datasetMetadata -> getActivePartitionList(datasetMetadata).size() > 0)
        .collect(Collectors.toList());
  }

  /** Gets the active list of partitions from the provided dataset metadata */
  protected static List<Integer> getActivePartitionList(DatasetMetadata datasetMetadata) {
    Optional<DatasetPartitionMetadata> datasetPartitionMetadata =
        datasetMetadata
            .getPartitionConfigs()
            .stream()
            .filter(partitionMetadata -> partitionMetadata.getEndTimeEpochMs() == MAX_TIME)
            .findFirst();

    if (datasetPartitionMetadata.isEmpty()) {
      return Collections.emptyList();
    }
    return datasetPartitionMetadata
        .get()
        .getPartitions()
        .stream()
        .map(Integer::parseInt)
        .collect(Collectors.toUnmodifiableList());
  }

  // we sort the datasets to rank from which dataset do we start matching candidate service names
  // in the future we can change the ordering from sort to something else
  public static List<DatasetMetadata> sortDatasetsOnThroughput(
      List<DatasetMetadata> datasetMetadataList) {
    return datasetMetadataList
        .stream()
        .sorted(Comparator.comparingLong(DatasetMetadata::getThroughputBytes).reversed())
        .collect(Collectors.toList());
  }

  /** Builds a Properties hashtable using the provided config, and sensible defaults */
  protected static Properties makeKafkaStreamsProps(
      KaldbConfigs.PreprocessorConfig.KafkaStreamConfig kafkaStreamConfig) {
    checkArgument(
        !kafkaStreamConfig.getApplicationId().isEmpty(),
        "Kafka stream applicationId must be provided");
    checkArgument(
        !kafkaStreamConfig.getBootstrapServers().isEmpty(),
        "Kafka stream bootstrapServers must be provided");
    checkArgument(
        kafkaStreamConfig.getNumStreamThreads() > 0,
        "Kafka stream numStreamThreads must be greater than 0");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamConfig.getApplicationId());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStreamConfig.getBootstrapServers());
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, kafkaStreamConfig.getNumStreamThreads());

    // These props allow using brokers versions back to 2.0, by reverting breaking changes
    //   introduced in the client versions 3.0+
    //   https://www.confluent.io/blog/apache-kafka-3-0-major-improvements-and-new-features/

    // https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=177050750
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

    // https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default
    props.put(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), false);
    props.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "1");

    // This will allow parallel processing up to the amount of upstream partitions. You cannot have
    // more threads than you have upstreams due to how the work is partitioned
    props.put(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, kafkaStreamConfig.getProcessingGuarantee());

    // don't override any property we already set
    for (Map.Entry<String, String> additionalProp :
        kafkaStreamConfig.getAdditionalPropsMap().entrySet()) {
      maybeOverride(props, additionalProp.getKey(), additionalProp.getValue(), false);
    }

    return props;
  }
}
