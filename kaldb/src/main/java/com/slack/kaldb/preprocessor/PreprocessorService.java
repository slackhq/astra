package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.metadata.service.ServiceMetadata;
import com.slack.kaldb.metadata.service.ServiceMetadataStore;
import com.slack.kaldb.metadata.service.ServicePartitionMetadata;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
 * ServiceMetadataStore, with the upstream topic, downstream topic, and transforms stored in the
 * service config.
 *
 * <p>Changes to the ServiceMetadata will cause the existing Kafka Stream topology to be closed, and
 * this service will restart consumption with a new stream topology representing the newly updated
 * metadata. This class implements a doStart/doStop similar to the AbstractIdleService, but by
 * extending an AbstractService we also gain access to notifyFailed() in the event a subsequent load
 * were to fail.
 */
public class PreprocessorService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(PreprocessorService.class);
  private static final long MAX_TIME = Long.MAX_VALUE;

  private static final boolean INITIALIZE_RATE_LIMIT_WARM = true;

  private final ServiceMetadataStore serviceMetadataStore;
  private final PreprocessorRateLimiter rateLimiter;
  private final Properties kafkaProperties;
  private final List<String> upstreamTopics;
  private final String downstreamTopic;
  private final String dataTransformer;
  private final MeterRegistry meterRegistry;

  private KafkaStreams kafkaStreams;
  private KafkaStreamsMetrics kafkaStreamsMetrics;

  private final Timer configReloadTimer;
  public static final String CONFIG_RELOAD_TIMER = "preprocessor_config_reload_timer";

  public PreprocessorService(
      ServiceMetadataStore serviceMetadataStore,
      KaldbConfigs.PreprocessorConfig preprocessorConfig,
      MeterRegistry meterRegistry) {
    this.serviceMetadataStore = serviceMetadataStore;
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
    serviceMetadataStore.addListener(this::load);
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
    serviceMetadataStore.removeListener(this::load);
    LOG.info("Preprocessor service closed");
  }

  /**
   * Configures and starts a KafkaStream processor, based off of the cached ServiceMetadataStore.
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

      // only attempt to register stream processing on valid service configurations
      List<ServiceMetadata> serviceMetadataToProcess =
          filterValidServiceMetadata(serviceMetadataStore.listSync());

      if (serviceMetadataToProcess.size() > 0) {
        Topology topology =
            buildTopology(
                serviceMetadataToProcess,
                rateLimiter,
                upstreamTopics,
                downstreamTopic,
                dataTransformer);
        kafkaStreams = new KafkaStreams(topology, kafkaProperties);
        kafkaStreamsMetrics = new KafkaStreamsMetrics(kafkaStreams);
        kafkaStreamsMetrics.bindTo(meterRegistry);
        kafkaStreams.start();
        LOG.info("Kafka stream processor config loaded successfully");
      } else {
        LOG.info(
            "No valid service configurations found to process - will retry on next service configuration update");
      }
      loadTimer.stop(configReloadTimer);
    } catch (Exception e) {
      notifyFailed(e);
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
      List<String> upstreamTopics,
      String downstreamTopic,
      String dataTransformer) {
    Preconditions.checkArgument(
        !serviceMetadataList.isEmpty(), "service metadata list must not be empty");
    Preconditions.checkArgument(upstreamTopics.size() > 0, "upstream topic list must not be empty");
    Preconditions.checkArgument(!downstreamTopic.isEmpty(), "downstream topic must not be empty");
    Preconditions.checkArgument(!dataTransformer.isEmpty(), "data transformer must not be empty");

    StreamsBuilder builder = new StreamsBuilder();

    ValueMapper<byte[], Iterable<Trace.Span>> valueMapper =
        PreprocessorValueMapper.byteArrayToTraceSpans(dataTransformer);

    StreamPartitioner<String, Trace.Span> streamPartitioner =
        streamPartitioner(
            serviceMetadataList
                .stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        KaldbMetadata::getName, PreprocessorService::getActivePartitionList)));

    Predicate<String, Trace.Span> rateLimitPredicate =
        rateLimiter.createRateLimiter(
            serviceMetadataList
                .stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        KaldbMetadata::getName, ServiceMetadata::getThroughputBytes)));

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
        .map(Integer::parseInt)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Returns a StreamPartitioner that selects from the provided list of service metadata. If no
   * valid service metadata are provided throws an exception.
   */
  protected static StreamPartitioner<String, Trace.Span> streamPartitioner(
      Map<String, List<Integer>> serviceNameToPartitionList) {
    checkArgument(
        serviceNameToPartitionList.entrySet().size() > 0,
        "serviceNameToPartitionList cannot be empty");
    checkArgument(
        serviceNameToPartitionList.keySet().stream().noneMatch(String::isEmpty),
        "serviceNameToPartitionList cannot have any empty keys");
    checkArgument(
        serviceNameToPartitionList.values().stream().noneMatch(List::isEmpty),
        "serviceNameToPartitionList cannot have any empty partition lists");

    return (topic, key, value, partitionCount) -> {
      String serviceName = PreprocessorValueMapper.getServiceName(value);
      if (!serviceNameToPartitionList.containsKey(serviceName)) {
        // this shouldn't happen, as we should have filtered all the missing services in the value
        // mapper stage
        throw new IllegalStateException(
            String.format("Service '%s' was not found in service metadata", serviceName));
      }

      List<Integer> partitions = serviceNameToPartitionList.getOrDefault(serviceName, List.of());
      return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
    };
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

    // This will allow parallel processing up to the amount of upstream partitions. You cannot have
    // more threads than you have upstreams due to how the work is partitioned
    props.put(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, kafkaStreamConfig.getProcessingGuarantee());

    return props;
  }
}
