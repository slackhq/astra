package com.slack.kaldb.preprocessor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.service.ServiceMetadata;
import com.slack.kaldb.metadata.service.ServiceMetadataStore;
import com.slack.kaldb.metadata.service.ServicePartitionMetadata;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PreprocessorService consumes from multiple upstream topics, applies rate limiting, transforms
 * the data format, and then writes out the new message to a common downstream topic targeting
 * specific partitions. The upstream topic information, rate limits, and target partitions are read
 * in via the ServiceMetadataStore, and the common output topic and transforms are stored via a
 * service config.
 */
public class PreprocessorService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(PreprocessorService.class);
  private static final long MAX_TIME = Long.MAX_VALUE;

  private final ServiceMetadataStore serviceMetadataStore;
  private final PreprocessorRateLimiter rateLimiter;
  private final Properties kafkaProperties;
  private final String downstreamTopic;
  private final String dataTransformer;
  private final MeterRegistry meterRegistry;

  private KafkaStreams kafkaStreams;
  private KafkaStreamsMetrics kafkaStreamsMetrics;

  private final Counter configReloadCounter;
  public static final String CONFIG_RELOAD_COUNTER = "preprocessor_config_reload_counter";

  public PreprocessorService(
      ServiceMetadataStore serviceMetadataStore,
      KaldbConfigs.PreprocessorConfig preprocessorConfig,
      MeterRegistry meterRegistry) {
    this.serviceMetadataStore = serviceMetadataStore;
    this.meterRegistry = meterRegistry;
    this.configReloadCounter = meterRegistry.counter(CONFIG_RELOAD_COUNTER);

    this.kafkaProperties = makeKafkaStreamsProps(preprocessorConfig.getKafkaStreamConfig());
    this.downstreamTopic = preprocessorConfig.getKafkaStreamConfig().getDownstreamTopic();
    this.dataTransformer = preprocessorConfig.getDataTransformer();
    this.rateLimiter =
        new PreprocessorRateLimiter(
            meterRegistry, preprocessorConfig.getPreprocessorInstanceCount());
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting preprocessor service");
    load();
    serviceMetadataStore.addListener(this::load);
    LOG.info("Preprocessor service started");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping preprocessor service");
    if (kafkaStreams != null) {
      kafkaStreams.close(DEFAULT_START_STOP_DURATION);
    }
    if (kafkaStreamsMetrics != null) {
      kafkaStreamsMetrics.close();
    }
    serviceMetadataStore.removeListener(this::load);
    LOG.info("Preprocessor service closed");
  }

  /**
   * Configures and starts a KafkaStream processor, based off of the cached ServiceMetadataStore.
   * This method is reentrant, and will restart any existing KafkaStream processors. Access to this
   * must be synchronized if using this method as part of a listener.
   */
  public synchronized void load() {
    configReloadCounter.increment();
    LOG.info("Loading new Kafka stream processor config");
    if (kafkaStreams != null) {
      LOG.info("Closing existing Kafka stream processor");
      kafkaStreams.close();
      kafkaStreams.cleanUp();
    }
    if (kafkaStreamsMetrics != null) {
      kafkaStreamsMetrics.close();
    }

    // only attempt to register stream processing on valid service configurations
    List<ServiceMetadata> serviceMetadataToProcess =
        filterValidServiceMetadata(serviceMetadataStore.listSync());

    if (serviceMetadataToProcess.size() > 0) {
      Topology topology =
          buildTopology(serviceMetadataToProcess, rateLimiter, downstreamTopic, dataTransformer);
      kafkaStreams = new KafkaStreams(topology, kafkaProperties);
      kafkaStreamsMetrics = new KafkaStreamsMetrics(kafkaStreams);
      kafkaStreamsMetrics.bindTo(meterRegistry);
      kafkaStreams.start();
      LOG.info("Kafka stream processor config loaded successfully");
    } else {
      LOG.info(
          "No valid service configurations found to process - will retry on next service configuration update");
    }
  }

  /**
   * Builds a KafkaStream Topology with multiple source topics, targeting a single sink. Applies
   * rate limits per-service if required, and uses the service configured target topic sink and data
   * transformer.
   */
  protected static Topology buildTopology(
      List<ServiceMetadata> serviceMetadataList,
      PreprocessorRateLimiter rateLimiter,
      String downstreamTopic,
      String dataTransformer) {
    Preconditions.checkArgument(
        !serviceMetadataList.isEmpty(), "service metadata list must not be empty");
    Preconditions.checkArgument(!downstreamTopic.isEmpty(), "downstream topic must not be empty");
    Preconditions.checkArgument(!dataTransformer.isEmpty(), "data transformer must not be empty");

    StreamsBuilder builder = new StreamsBuilder();
    ValueMapper<byte[], Trace.ListOfSpans> valueMapper =
        PreprocessorValueMapper.byteArrayToTraceListOfSpans(dataTransformer);
    serviceMetadataList.forEach(
        (serviceMetadata ->
            builder
                .stream(
                    serviceMetadata.getName(), Consumed.with(Serdes.String(), Serdes.ByteArray()))
                .filter(
                    rateLimiter.createRateLimiter(
                        serviceMetadata.getName(), serviceMetadata.getThroughputBytes()))
                .mapValues(valueMapper)
                .filter((key, listOfSpans) -> listOfSpans != null)
                .peek(
                    (key, listOfSpans) ->
                        LOG.debug(
                            "Processed key/record {}/{} from topic {} to topic {}",
                            key,
                            listOfSpans.toString(),
                            serviceMetadata.getName(),
                            downstreamTopic))
                .to(
                    downstreamTopic,
                    Produced.with(
                        Serdes.String(),
                        KaldbSerdes.TraceListOfSpans(),
                        streamPartitioner(getActivePartitionList(serviceMetadata))))));

    return builder.build();
  }

  /**
   * Filters the provided list of service metadata to those that are valid. This includes correctly
   * defined throughput and partition configurations.
   */
  protected static List<ServiceMetadata> filterValidServiceMetadata(
      List<ServiceMetadata> serviceMetadataList) {
    return serviceMetadataList
        .stream()
        .filter(serviceMetadata -> serviceMetadata.getThroughputBytes() > 0)
        .filter(serviceMetadata -> getActivePartitionList(serviceMetadata).size() > 0)
        .collect(Collectors.toList());
  }

  /** Gets the active list of partitions from the provided service metadata */
  protected static List<Integer> getActivePartitionList(ServiceMetadata serviceMetadata) {
    Optional<ServicePartitionMetadata> servicePartitionMetadata =
        serviceMetadata
            .getPartitionConfigs()
            .stream()
            .filter(partitionMetadata -> partitionMetadata.getEndTimeEpochMs() == MAX_TIME)
            .findFirst();

    if (servicePartitionMetadata.isEmpty()) {
      return Collections.emptyList();
    }
    return servicePartitionMetadata
        .get()
        .getPartitions()
        .stream()
        .map(java.lang.Integer::parseInt)
        .collect(Collectors.toList());
  }

  /**
   * Returns a StreamPartitioner that selects from the provided list of partitions. If no valid
   * partitions are provided throws an exception.
   */
  protected static StreamPartitioner<Object, Object> streamPartitioner(List<Integer> partitions) {
    checkArgument(partitions.size() > 0, "Invalid partition list provided, had no partitions");
    checkArgument(
        partitions.stream().noneMatch(integer -> integer < 0),
        "All partitions must be positive, non-null values");
    return (topic, key, value, partitionCount) ->
        partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
  }

  /** Builds a Properties hashtable using the provided config, and sensible defaults */
  protected static Properties makeKafkaStreamsProps(
      KaldbConfigs.PreprocessorConfig.KafkaStreamConfig kafkaStreamConfig) {
    Preconditions.checkArgument(
        !kafkaStreamConfig.getApplicationId().isEmpty(),
        "Kafka stream applicationId must be provided");
    Preconditions.checkArgument(
        !kafkaStreamConfig.getBootstrapServers().isEmpty(),
        "Kafka stream bootstrapServers must be provided");
    Preconditions.checkArgument(
        kafkaStreamConfig.getNumStreamThreads() > 0,
        "Kafka stream numStreamThreads must be greater than 0");

    Properties props = new Properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamConfig.getApplicationId());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStreamConfig.getBootstrapServers());
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, kafkaStreamConfig.getNumStreamThreads());

    // todo - expose this as an option for users to configure?
    // This will allow parallel processing up to the amount of upstream partitions. You cannot have
    // more threads than you have upstreams due to how the work is partitioned
    // @see StreamsConfig.EXACTLY_ONCE
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);

    return props;
  }
}
