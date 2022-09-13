package com.slack.kaldb.zipkinApi;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.ZK_PATH_PREFIX;
import static com.slack.kaldb.testlib.KaldbSearchUtils.searchUsingGrpcApi;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.slack.kaldb.chunkManager.RollOverChunkTask;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.Kaldb;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class ZipkinServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(ZipkinServiceTest.class);

  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static final String TEST_KAFKA_TOPIC_1 = "test-topic-1";
  private static final String KALDB_TEST_CLIENT_1 = "kaldb-test-client1";

  private DatasetMetadataStore datasetMetadataStore;
  private ZookeeperMetadataStoreImpl zkMetadataStore;
  private PrometheusMeterRegistry meterRegistry;

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private TestKafkaServer kafkaServer;
  private TestingServer zkServer;
  private S3Client s3Client;

  @Before
  public void setUp() throws Exception {
    zkServer = new TestingServer();
    kafkaServer = new TestKafkaServer();
    s3Client = S3_MOCK_RULE.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(TEST_S3_BUCKET).build());

    // We side load a service metadata entry telling it to create an entry with the partitions that
    // we use in test
    meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix(ZK_PATH_PREFIX)
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();
    zkMetadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);
    datasetMetadataStore = new DatasetMetadataStore(zkMetadataStore, true);
    final DatasetPartitionMetadata partition =
        new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0", "1"));
    final List<DatasetPartitionMetadata> partitionConfigs = Collections.singletonList(partition);
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(MessageUtil.TEST_DATASET_NAME, "serviceOwner", 1000, partitionConfigs);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);
  }

  @After
  public void teardown() throws Exception {
    kafkaServer.close();
    meterRegistry.close();
    datasetMetadataStore.close();
    zkMetadataStore.close();
    zkServer.close();
  }

  @Test
  public void testDistributedQueryOneIndexerOneQueryNode() throws Exception {
    assertThat(kafkaServer.getBroker().isRunning()).isTrue();

    LOG.info("Starting query service");
    int queryServicePort = 8887;
    KaldbConfigs.KaldbConfig queryServiceConfig =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + kafkaServer.getBroker().getKafkaPort().get(),
            -1,
            TEST_KAFKA_TOPIC_1,
            0,
            KALDB_TEST_CLIENT_1,
            TEST_S3_BUCKET,
            queryServicePort,
            zkServer.getConnectString(),
            ZK_PATH_PREFIX,
            KaldbConfigs.NodeRole.QUERY,
            1000,
            "api_Log",
            -1);
    Kaldb queryService = new Kaldb(queryServiceConfig, meterRegistry);
    queryService.start();
    queryService.serviceManager.awaitHealthy(DEFAULT_START_STOP_DURATION);

    int indexerPort = 10000;
    LOG.info(
        "Creating indexer service at port {}, topic: {} and partition {}",
        indexerPort,
        TEST_KAFKA_TOPIC_1,
        0);
    // create a kaldb indexer
    KaldbConfigs.KaldbConfig indexerConfig =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + kafkaServer.getBroker().getKafkaPort().get(),
            indexerPort,
            TEST_KAFKA_TOPIC_1,
            0,
            KALDB_TEST_CLIENT_1,
            TEST_S3_BUCKET,
            -1,
            zkServer.getConnectString(),
            ZK_PATH_PREFIX,
            KaldbConfigs.NodeRole.INDEX,
            1000,
            "api_log",
            9003);

    PrometheusMeterRegistry indexerMeterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    Kaldb indexer = new Kaldb(indexerConfig, s3Client, indexerMeterRegistry);
    indexer.start();
    indexer.serviceManager.awaitHealthy(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);

    // Produce messages to kafka, so the indexer can consume them.

    final Instant indexedMessagesStartTime = Instant.now().minus(5, ChronoUnit.MINUTES);
    final int indexedMessagesCount =
        produceMessagesToKafka(
            kafkaServer.getBroker(), indexedMessagesStartTime, TEST_KAFKA_TOPIC_1, 0);

    await()
        .until(
            () -> {
              try {
                double count = getCount(MESSAGES_RECEIVED_COUNTER, indexerMeterRegistry);
                LOG.debug("Registry1 current_count={} total_count={}", count, indexedMessagesCount);
                return count == indexedMessagesCount;
              } catch (MeterNotFoundException e) {
                return false;
              }
            });

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, indexerMeterRegistry) == 1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, indexerMeterRegistry)).isZero();

    // Query from the grpc search service
    KaldbSearch.SearchResult queryServiceSearchResponse =
        searchUsingGrpcApi("*:*", queryServicePort, 0, Instant.now().toEpochMilli());

    assertThat(queryServiceSearchResponse.getTotalNodes()).isEqualTo(1);
    assertThat(queryServiceSearchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(queryServiceSearchResponse.getTotalCount()).isEqualTo(100);
    assertThat(queryServiceSearchResponse.getHitsCount()).isEqualTo(100);

    // Query from the zipkin search service
    String endpoint = "http://127.0.0.1:" + queryServicePort;
    WebClient webClient = WebClient.of(endpoint);
    AggregatedHttpResponse response = webClient.get("/api/v2/trace/1").aggregate().join();
    String body = response.content(StandardCharsets.UTF_8);
    assertThat(response.status().code()).isEqualTo(200);
    String expectedTrace =
        String.format(
            "[{\n"
                + "  \"traceId\": \"1\",\n"
                + "  \"parentId\": \"1\",\n"
                + "  \"id\": \"localhost:100:0\",\n"
                + "  \"name\": \"testDataSet\",\n"
                + "  \"serviceName\": \"\",\n"
                + "  \"timestamp\": \"%d\",\n"
                + "  \"duration\": \"5000\",\n"
                + "  \"tags\": {\n"
                + "    \"longproperty\": \"1\",\n"
                + "    \"floatproperty\": \"1.0\",\n"
                + "    \"hostname\": \"localhost\",\n"
                + "    \"intproperty\": \"1\",\n"
                + "    \"message\": \"The identifier in this message is Message1\",\n"
                + "    \"doubleproperty\": \"1.0\"\n"
                + "  },\n"
                + "  \"remoteEndpoint\": {\n"
                + "    \"serviceName\": \"testDataSet\",\n"
                + "    \"ipv4\": \"\",\n"
                + "    \"ipv6\": \"\",\n"
                + "    \"port\": 0\n"
                + "  }\n"
                + "}]",
            ZipkinService.convertToMicroSeconds(indexedMessagesStartTime));
    assertThat(body).isEqualTo(expectedTrace);

    // Shutdown
    LOG.info("Shutting down query service.");
    queryService.shutdown();
    LOG.info("Shutting down indexer.");
    indexer.shutdown();
  }
}
