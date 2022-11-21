package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.preprocessor.PreprocessorValueMapper.SERVICE_NAME_KEY;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.MetricsUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.junit.Test;

public class PreprocessorServiceUnitTest {

  @Test
  public void shouldBuildValidPropsFromStreamConfig() {
    String applicationId = "applicationId";
    String bootstrapServers = "bootstrap";
    String processingGuarantee = "at_least_once";
    int replicationFactor = 1;
    boolean enableIdempotence = false;
    String acksConfig = "1";
    int numStreamThreads = 1;

    KaldbConfigs.PreprocessorConfig.KafkaStreamConfig kafkaStreamConfig =
        KaldbConfigs.PreprocessorConfig.KafkaStreamConfig.newBuilder()
            .setApplicationId(applicationId)
            .setBootstrapServers(bootstrapServers)
            .setNumStreamThreads(numStreamThreads)
            .setProcessingGuarantee(processingGuarantee)
            .build();

    Properties properties = PreprocessorService.makeKafkaStreamsProps(kafkaStreamConfig);
    assertThat(properties.size()).isEqualTo(7);

    assertThat(properties.get(StreamsConfig.APPLICATION_ID_CONFIG)).isEqualTo(applicationId);

    assertThat(properties.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo(bootstrapServers);

    assertThat(properties.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG)).isEqualTo(numStreamThreads);
    assertThat(properties.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))
        .isEqualTo(processingGuarantee);
    assertThat(properties.get(StreamsConfig.REPLICATION_FACTOR_CONFIG))
        .isEqualTo(replicationFactor);
    assertThat(
            properties.get(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)))
        .isEqualTo(enableIdempotence);
    assertThat(properties.get(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG)))
        .isEqualTo(acksConfig);
  }

  @Test
  public void shouldPreventInvalidStreamPropsConfig() {
    String applicationId = "applicationId";
    String bootstrapServers = "bootstrap";
    int numStreamThreads = 1;

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              KaldbConfigs.PreprocessorConfig.KafkaStreamConfig kafkaStreamConfig =
                  KaldbConfigs.PreprocessorConfig.KafkaStreamConfig.newBuilder()
                      .setBootstrapServers(bootstrapServers)
                      .setNumStreamThreads(numStreamThreads)
                      .build();
              PreprocessorService.makeKafkaStreamsProps(kafkaStreamConfig);
            });

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              KaldbConfigs.PreprocessorConfig.KafkaStreamConfig kafkaStreamConfig =
                  KaldbConfigs.PreprocessorConfig.KafkaStreamConfig.newBuilder()
                      .setApplicationId(applicationId)
                      .setNumStreamThreads(numStreamThreads)
                      .build();
              PreprocessorService.makeKafkaStreamsProps(kafkaStreamConfig);
            });

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              KaldbConfigs.PreprocessorConfig.KafkaStreamConfig kafkaStreamConfig =
                  KaldbConfigs.PreprocessorConfig.KafkaStreamConfig.newBuilder()
                      .setApplicationId(applicationId)
                      .setBootstrapServers(bootstrapServers)
                      .build();
              PreprocessorService.makeKafkaStreamsProps(kafkaStreamConfig);
            });

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              KaldbConfigs.PreprocessorConfig.KafkaStreamConfig kafkaStreamConfig =
                  KaldbConfigs.PreprocessorConfig.KafkaStreamConfig.newBuilder()
                      .setApplicationId(applicationId)
                      .setBootstrapServers(bootstrapServers)
                      .setNumStreamThreads(0)
                      .build();
              PreprocessorService.makeKafkaStreamsProps(kafkaStreamConfig);
            });
  }

  @Test
  public void shouldCorrectlyThroughputSortDatasets() {
    DatasetMetadata datasetMetadata1 =
        new DatasetMetadata(
            "service1",
            "service1",
            1,
            List.of(new DatasetPartitionMetadata(100, 200, List.of("0"))),
            "no_service_matching_docs");
    DatasetMetadata datasetMetadata2 =
        new DatasetMetadata(
            "service2",
            "service2",
            3,
            List.of(new DatasetPartitionMetadata(100, 200, List.of("0"))),
            DatasetMetadata.MATCH_ALL_SERVICE);
    DatasetMetadata datasetMetadata3 =
        new DatasetMetadata(
            "service3",
            "service3",
            2,
            List.of(new DatasetPartitionMetadata(100, 200, List.of("0"))),
            DatasetMetadata.MATCH_ALL_SERVICE);

    List<DatasetMetadata> throughputSortedDatasets =
        PreprocessorService.sortDatasetsOnThroughput(
            List.of(datasetMetadata1, datasetMetadata2, datasetMetadata3));
    assertThat(throughputSortedDatasets.size()).isEqualTo(3);
    assertThat(throughputSortedDatasets.get(0).getName()).isEqualTo("service2");
    assertThat(throughputSortedDatasets.get(1).getName()).isEqualTo("service3");
    assertThat(throughputSortedDatasets.get(2).getName()).isEqualTo("service1");
  }

  @Test
  public void shouldReturnRandomPartitionFromStreamPartitioner() {
    String datasetName = "datasetName";
    List<Integer> partitionList = List.of(33, 44, 55);
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            datasetName,
            datasetName,
            1,
            List.of(new DatasetPartitionMetadata(100, Long.MAX_VALUE, List.of("33", "44", "55"))),
            datasetName);
    StreamPartitioner<String, Trace.Span> streamPartitioner =
        PreprocessorService.streamPartitioner(List.of(datasetMetadata));

    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(
                Trace.KeyValue.newBuilder().setKey(SERVICE_NAME_KEY).setVStr(datasetName).build())
            .build();

    // all arguments except value are currently unused for determining the partition to assign, as
    // this comes the internal partition list that is set on stream partitioner initialization
    assertThat(partitionList.contains(streamPartitioner.partition("topic", null, span, 0)))
        .isTrue();
    assertThat(partitionList.contains(streamPartitioner.partition("topic", null, span, 1)))
        .isTrue();
    assertThat(partitionList.contains(streamPartitioner.partition("topic", "", span, 0))).isTrue();
    assertThat(partitionList.contains(streamPartitioner.partition("", null, span, 0))).isTrue();
  }

  @Test
  public void shouldFilterInvalidConfigurationsFromServiceMetadata() {
    DatasetMetadata validDatasetMetadata =
        new DatasetMetadata(
            "valid",
            "owner1",
            1000,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1"))),
            "valid");

    List<DatasetMetadata> datasetMetadataList =
        List.of(
            new DatasetMetadata(
                "invalidServicePartitionList",
                "owner1",
                1000,
                List.of(),
                "invalidServicePartitionList"),
            new DatasetMetadata(
                "invalidThroughputBytes",
                "owner1",
                0,
                List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1"))),
                "invalidThroughputBytes"),
            new DatasetMetadata(
                "invalidActivePartitions",
                "owner1",
                1000,
                List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of())),
                "invalidActivePartitions"),
            new DatasetMetadata(
                "invalidNoActivePartitions",
                "owner1",
                1000,
                List.of(
                    new DatasetPartitionMetadata(1, Instant.now().toEpochMilli(), List.of("1"))),
                "invalidNoActivePartitions"),
            validDatasetMetadata);

    List<DatasetMetadata> datasetMetadata1 =
        PreprocessorService.filterValidDatasetMetadata(datasetMetadataList);

    assertThat(datasetMetadata1.size()).isEqualTo(1);
    assertThat(datasetMetadata1.contains(validDatasetMetadata)).isTrue();

    Collections.shuffle(datasetMetadata1);

    List<DatasetMetadata> datasetMetadata2 =
        PreprocessorService.filterValidDatasetMetadata(datasetMetadataList);

    assertThat(datasetMetadata2.size()).isEqualTo(1);
    assertThat(datasetMetadata2.contains(validDatasetMetadata)).isTrue();

    List<DatasetMetadata> datasetMetadata3 =
        PreprocessorService.filterValidDatasetMetadata(List.of());
    assertThat(datasetMetadata3.size()).isEqualTo(0);
  }

  @Test
  public void shouldGetActivePartitionsFromServiceMetadata() {
    DatasetMetadata datasetMetadataEmptyPartitions =
        new DatasetMetadata("empty", "owner1", 1000, List.of(), "empty");
    DatasetMetadata datasetMetadataNoActivePartitions =
        new DatasetMetadata(
            "empty",
            "owner1",
            1000,
            List.of(
                new DatasetPartitionMetadata(1, Instant.now().toEpochMilli(), List.of("1", "2"))),
            "empty");

    DatasetMetadata datasetMetadataNoPartitions =
        new DatasetMetadata(
            "empty",
            "owner1",
            1000,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of())),
            "empty");

    DatasetMetadata datasetMetadataMultiplePartitions =
        new DatasetMetadata(
            "empty",
            "owner1",
            1000,
            List.of(
                new DatasetPartitionMetadata(1, 10000, List.of("3", "4")),
                new DatasetPartitionMetadata(10001, Long.MAX_VALUE, List.of("5", "6"))),
            "empty");

    assertThat(PreprocessorService.getActivePartitionList(datasetMetadataEmptyPartitions))
        .isEqualTo(List.of());
    assertThat(PreprocessorService.getActivePartitionList(datasetMetadataNoActivePartitions))
        .isEqualTo(List.of());
    assertThat(PreprocessorService.getActivePartitionList(datasetMetadataNoPartitions))
        .isEqualTo(List.of());
    assertThat(PreprocessorService.getActivePartitionList(datasetMetadataMultiplePartitions))
        .isEqualTo(List.of(5, 6));
  }

  @Test
  public void shouldBuildStreamTopology() {
    List<DatasetMetadata> datasetMetadata =
        List.of(
            new DatasetMetadata(
                "dataset1",
                "owner1",
                1000,
                List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1", "2"))),
                "dataset1"),
            new DatasetMetadata(
                "dataset2",
                "owner1",
                1000,
                List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1", "2"))),
                "dataset2"));

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    int preprocessorCount = 1;
    int maxBurstSeconds = 1;
    boolean initializeWarm = false;
    PreprocessorRateLimiter rateLimiter =
        new PreprocessorRateLimiter(
            meterRegistry, preprocessorCount, maxBurstSeconds, initializeWarm);

    List<String> upstreamTopics = List.of("upstream1", "upstream2", "upstream3");
    String downstreamTopic = "downstream";
    String dataTransformer = "api_log";
    Topology topology =
        PreprocessorService.buildTopology(
            datasetMetadata, rateLimiter, upstreamTopics, downstreamTopic, dataTransformer);

    // we have limited visibility into the topology, so we just verify we have the correct number of
    // stream processors as we expect
    assertThat(topology.describe().subtopologies().size()).isEqualTo(upstreamTopics.size());
  }

  @Test
  public void shouldThrowOnInvalidTopologyConfigs() {
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            "dataset1",
            "owner1",
            1000,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1", "2"))),
            "dataset1");

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    int preprocessorCount = 1;
    int maxBurstSeconds = 1;
    boolean initializeWarm = false;
    PreprocessorRateLimiter rateLimiter =
        new PreprocessorRateLimiter(
            meterRegistry, preprocessorCount, maxBurstSeconds, initializeWarm);
    List<String> upstreamTopics = List.of("upstream");
    String downstreamTopic = "downstream";
    String dataTransformer = "api_log";

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PreprocessorService.buildTopology(
                    List.of(), rateLimiter, upstreamTopics, downstreamTopic, dataTransformer));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PreprocessorService.buildTopology(
                    List.of(datasetMetadata),
                    rateLimiter,
                    List.of(),
                    downstreamTopic,
                    dataTransformer));
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                PreprocessorService.buildTopology(
                    List.of(datasetMetadata),
                    null,
                    upstreamTopics,
                    downstreamTopic,
                    dataTransformer));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PreprocessorService.buildTopology(
                    List.of(datasetMetadata), rateLimiter, upstreamTopics, "", dataTransformer));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PreprocessorService.buildTopology(
                    List.of(datasetMetadata), rateLimiter, upstreamTopics, downstreamTopic, ""));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PreprocessorService.buildTopology(
                    List.of(datasetMetadata),
                    rateLimiter,
                    upstreamTopics,
                    downstreamTopic,
                    "invalid"));
  }

  @Test
  public void shouldHandleEmptyDatasetMetadata() throws TimeoutException {
    DatasetMetadataStore datasetMetadataStore = mock(DatasetMetadataStore.class);
    when(datasetMetadataStore.listSync()).thenReturn(List.of());

    KaldbConfigs.PreprocessorConfig.KafkaStreamConfig kafkaStreamConfig =
        KaldbConfigs.PreprocessorConfig.KafkaStreamConfig.newBuilder()
            .setApplicationId("applicationId")
            .setBootstrapServers("bootstrap")
            .setNumStreamThreads(1)
            .build();
    KaldbConfigs.ServerConfig serverConfig =
        KaldbConfigs.ServerConfig.newBuilder()
            .setServerPort(8080)
            .setServerAddress("localhost")
            .build();
    KaldbConfigs.PreprocessorConfig preprocessorConfig =
        KaldbConfigs.PreprocessorConfig.newBuilder()
            .setKafkaStreamConfig(kafkaStreamConfig)
            .setServerConfig(serverConfig)
            .setPreprocessorInstanceCount(1)
            .setDataTransformer("api_log")
            .setRateLimiterMaxBurstSeconds(1)
            .build();

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    PreprocessorService preprocessorService =
        new PreprocessorService(datasetMetadataStore, preprocessorConfig, meterRegistry);

    preprocessorService.startAsync();
    preprocessorService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getTimerCount(PreprocessorService.CONFIG_RELOAD_TIMER, meterRegistry))
        .isEqualTo(1);

    preprocessorService.stopAsync();
    preprocessorService.awaitTerminated();
  }
}
