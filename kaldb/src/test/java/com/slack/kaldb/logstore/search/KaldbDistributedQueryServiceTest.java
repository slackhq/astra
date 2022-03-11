package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.KaldbConfigUtil.makeIndexerConfig;
import static com.slack.kaldb.testlib.KaldbConfigUtil.makeKafkaConfig;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.util.EventLoopGroups;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.server.KaldbIndexer;
import com.slack.kaldb.server.KaldbTimeoutLocalQueryService;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.testlib.TestingArmeriaServer;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbDistributedQueryServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(KaldbDistributedQueryServiceTest.class);

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static final SimpleMeterRegistry zkMetricsRegistry = new SimpleMeterRegistry();
  private static final SimpleMeterRegistry indexerMetricsRegistry1 = new SimpleMeterRegistry();
  private static final SimpleMeterRegistry indexerMetricsRegistry2 = new SimpleMeterRegistry();
  private static final SimpleMeterRegistry indexerMetricsRegistry3 = new SimpleMeterRegistry();
  private static final SimpleMeterRegistry queryMetricsRegistry = new SimpleMeterRegistry();

  private static KaldbServiceGrpc.KaldbServiceBlockingStub queryServiceStub;

  private static ServiceManager indexingServiceManager1;
  private static ServiceManager indexingServiceManager2;
  private static ServiceManager indexingServiceManager3;
  private static ServiceManager queryServiceManager1;

  private static final String TEST_KAFKA_TOPIC_1 = "test-topic-1";
  private static final String TEST_KAFKA_TOPIC_2 = "test-topic-2";
  private static final String TEST_KAFKA_TOPIC_3 = "test-topic-3";

  // we need 3 consumer groups to assert each indexer attaches to it's own consumer group
  // the reason being we need to assert if the consumer group is connected before sending messages
  // to kafka
  // else we can end up with missing messages and flaky tests
  private static final String KALDB_TEST_CLIENT_1 = "kaldb-test-client1";
  private static final String KALDB_TEST_CLIENT_2 = "kaldb-test-client2";
  private static final String KALDB_TEST_CLIENT_3 = "kaldb-test-client3";

  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static TestKafkaServer kafkaServer;

  private static TestingServer zkServer;
  private static MetadataStore metadataStore;
  private static SearchMetadataStore searchMetadataStore;
  private static KaldbDistributedQueryService queryService;

  private static EphemeralKafkaBroker broker;

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Before
  // TODO: This test is very similar to KaldbIndexerTest - explore a TestRule based setup
  public void initialize() throws Exception {
    Tracing.newBuilder().build();

    kafkaServer = new TestKafkaServer();
    zkServer = new TestingServer();
    zkServer.start();

    // TODO: Remove this additional config and use the bottom config instead?
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(zkMetricsRegistry, zkConfig);

    broker = kafkaServer.getBroker();
    assertThat(broker.isRunning()).isTrue();

    int indexServer1Port = 10000;
    SearchContext searchContext1 = new SearchContext("127.0.0.1", indexServer1Port);
    KaldbConfigs.KaldbConfig kaldbConfig1 =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + broker.getKafkaPort().get(),
            indexServer1Port,
            TEST_KAFKA_TOPIC_1,
            0,
            KALDB_TEST_CLIENT_1,
            TEST_S3_BUCKET,
            indexServer1Port,
            "",
            "",
            KaldbConfigs.NodeRole.INDEX,
            1000,
            "log_message");

    ChunkManagerUtil<LogMessage> chunkManagerUtil1 =
        new ChunkManagerUtil<>(
            S3_MOCK_RULE,
            indexerMetricsRegistry1,
            zkServer,
            10 * 1024 * 1024 * 1024L,
            100,
            searchContext1,
            metadataStore,
            kaldbConfig1.getIndexerConfig());

    // Produce messages to kafka, so the indexer can consume them.
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    int indexed1Count = produceMessagesToKafka(broker, startTime, TEST_KAFKA_TOPIC_1);

    indexingServiceManager1 =
        newIndexingServer(
            chunkManagerUtil1,
            kaldbConfig1,
            indexerMetricsRegistry1,
            0,
            makeKafkaConfig(
                TEST_KAFKA_TOPIC_1,
                0,
                KALDB_TEST_CLIENT_1,
                kafkaServer.getBroker().getBrokerList().get()));

    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);
    await()
        .until(
            () -> {
              try {
                double count = getCount(MESSAGES_RECEIVED_COUNTER, indexerMetricsRegistry1);
                LOG.debug("Registry1 current_count={} total_count={}", count, indexed1Count);
                return count == indexed1Count;
              } catch (MeterNotFoundException e) {
                return false;
              }
            });

    int indexServer2Port = 10001;
    SearchContext searchContext2 = new SearchContext("127.0.0.1", indexServer2Port);
    KaldbConfigs.KaldbConfig kaldbConfig2 =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + broker.getKafkaPort().get(),
            indexServer2Port,
            TEST_KAFKA_TOPIC_2,
            0,
            KALDB_TEST_CLIENT_2,
            TEST_S3_BUCKET,
            indexServer2Port,
            "",
            "",
            KaldbConfigs.NodeRole.INDEX,
            1000,
            "log_message");

    // Set it to the new config so that the new kafka writer picks up this config
    // KaldbConfig.initFromConfigObject(kaldbConfig2);
    ChunkManagerUtil<LogMessage> chunkManagerUtil2 =
        new ChunkManagerUtil<>(
            S3_MOCK_RULE,
            indexerMetricsRegistry2,
            zkServer,
            10 * 1024 * 1024 * 1024L,
            100,
            searchContext2,
            metadataStore,
            kaldbConfig2.getIndexerConfig());

    // Produce messages to kafka, so the indexer can consume them.
    final Instant startTime2 =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    int indexed2Count = produceMessagesToKafka(broker, startTime2, TEST_KAFKA_TOPIC_2);

    indexingServiceManager2 =
        newIndexingServer(
            chunkManagerUtil2,
            kaldbConfig2,
            indexerMetricsRegistry2,
            3000,
            makeKafkaConfig(
                TEST_KAFKA_TOPIC_2,
                0,
                KALDB_TEST_CLIENT_2,
                kafkaServer.getBroker().getBrokerList().get()));

    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 2);
    await()
        .until(
            () -> {
              try {
                double count = getCount(MESSAGES_RECEIVED_COUNTER, indexerMetricsRegistry2);
                LOG.debug("Registry2 current_count={} total_count={}", count, indexed2Count);
                return count == indexed2Count;
              } catch (MeterNotFoundException e) {
                return false;
              }
            });

    searchMetadataStore = new SearchMetadataStore(metadataStore, true);
    queryService = new KaldbDistributedQueryService(searchMetadataStore, queryMetricsRegistry);
    Server queryServer =
        Server.builder()
            .workerGroup(
                EventLoopGroups.newEventLoopGroup(4, "armeria-common-worker-query", true), true)
            // Hardcoding this could mean port collisions b/w tests running in parallel.
            .http(0)
            .verboseResponses(true)
            .service(GrpcService.builder().addService(queryService).build())
            .build();
    HashSet<Service> queryServices = new HashSet<>();
    queryServices.add(new TestingArmeriaServer(queryServer, queryMetricsRegistry));
    queryServiceManager1 = new ServiceManager(queryServices);
    queryServiceManager1.startAsync();
    // TODO: remove startAsync with blocking call and remove sleep
    Thread.sleep(1000);

    queryServiceStub =
        Clients.newClient(
            String.format("gproto+http://127.0.0.1:%s/", queryServer.activeLocalPort()),
            KaldbServiceGrpc.KaldbServiceBlockingStub.class);
  }

  private static ServiceManager newIndexingServer(
      ChunkManagerUtil<LogMessage> chunkManagerUtil,
      KaldbConfigs.KaldbConfig kaldbConfig,
      SimpleMeterRegistry meterRegistry,
      int waitForSearchMs,
      KaldbConfigs.KafkaConfig kafkaConfig)
      throws TimeoutException {

    HashSet<Service> services = new HashSet<>();

    KaldbIndexer indexer =
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            metadataStore,
            makeIndexerConfig(1000, "api_log"),
            kafkaConfig,
            meterRegistry);

    services.add(chunkManagerUtil.chunkManager);
    services.add(indexer);

    KaldbLocalQueryService<LogMessage> service =
        new KaldbLocalQueryService<>(chunkManagerUtil.chunkManager);
    Server server;
    if (waitForSearchMs > 0) {
      KaldbTimeoutLocalQueryService wrapperService =
          new KaldbTimeoutLocalQueryService(service, waitForSearchMs);
      server =
          Server.builder()
              .workerGroup(
                  EventLoopGroups.newEventLoopGroup(
                      4, "armeria-common-worker-indexer-delayed", true),
                  true)
              .http(kaldbConfig.getIndexerConfig().getServerConfig().getServerPort())
              .verboseResponses(true)
              .service(GrpcService.builder().addService(wrapperService).build())
              .build();
    } else {
      server =
          Server.builder()
              .workerGroup(
                  EventLoopGroups.newEventLoopGroup(4, "armeria-common-worker-indexer", true), true)
              .http(kaldbConfig.getIndexerConfig().getServerConfig().getServerPort())
              .verboseResponses(true)
              .service(GrpcService.builder().addService(service).build())
              .build();
    }
    services.add(new TestingArmeriaServer(server, meterRegistry));
    ServiceManager serviceManager = new ServiceManager(services);
    // make sure the services are up and running
    serviceManager.startAsync().awaitHealthy(DEFAULT_START_STOP_DURATION);
    return serviceManager;
  }

  @After
  public void shutdownServer() throws Exception {
    if (indexingServiceManager1 != null) {
      indexingServiceManager1.stopAsync().awaitStopped(30, TimeUnit.SECONDS);
    }
    if (indexingServiceManager2 != null) {
      indexingServiceManager2.stopAsync().awaitStopped(30, TimeUnit.SECONDS);
    }
    if (queryServiceManager1 != null) {
      queryServiceManager1.stopAsync().awaitStopped(30, TimeUnit.SECONDS);
    }
    if (searchMetadataStore != null) {
      searchMetadataStore.close();
    }
    if (metadataStore != null) {
      metadataStore.close();
    }
    zkMetricsRegistry.close();
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    if (zkServer != null) {
      zkServer.close();
    }
    if (broker != null) {
      broker.stop();
    }
  }

  @Test
  public void testSearch() throws Exception {
    KaldbSearch.SearchResult searchResponse =
        queryServiceStub.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("*:*")
                .setStartTimeEpochMs(0L)
                .setEndTimeEpochMs(1601547099000L)
                .setHowMany(100)
                .setBucketCount(2)
                .build());

    assertThat(searchResponse.getTotalNodes()).isEqualTo(2);
    assertThat(searchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(searchResponse.getTotalCount()).isEqualTo(200);
    assertThat(searchResponse.getHitsCount()).isEqualTo(100);

    verifyAddServerToZK();
    testSearchWithOneShardTimeout();
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private void verifyAddServerToZK() throws Exception {
    int indexServer3Port = 10003;
    SearchContext searchContext3 = new SearchContext("127.0.0.1", indexServer3Port);
    KaldbConfigs.KaldbConfig kaldbConfig3 =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + broker.getKafkaPort().get(),
            indexServer3Port,
            TEST_KAFKA_TOPIC_3,
            0,
            KALDB_TEST_CLIENT_3,
            TEST_S3_BUCKET,
            indexServer3Port,
            "",
            "",
            KaldbConfigs.NodeRole.INDEX,
            1000,
            "log_message");

    ChunkManagerUtil<LogMessage> chunkManagerUtil3 =
        new ChunkManagerUtil<>(
            S3_MOCK_RULE,
            indexerMetricsRegistry3,
            zkServer,
            10 * 1024 * 1024 * 1024L,
            100,
            searchContext3,
            metadataStore,
            kaldbConfig3.getIndexerConfig());

    // Produce messages to kafka, so the indexer can consume them.
    final Instant startTime3 =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    int indexed3Count = produceMessagesToKafka(broker, startTime3, TEST_KAFKA_TOPIC_3);

    indexingServiceManager3 =
        newIndexingServer(
            chunkManagerUtil3,
            kaldbConfig3,
            indexerMetricsRegistry3,
            0,
            makeKafkaConfig(
                TEST_KAFKA_TOPIC_3,
                0,
                KALDB_TEST_CLIENT_3,
                kafkaServer.getBroker().getBrokerList().get()));

    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 3);
    await()
        .until(
            () -> {
              try {
                double count = getCount(MESSAGES_RECEIVED_COUNTER, indexerMetricsRegistry3);
                LOG.debug("Registry3 current_count={} total_count={}", count, indexed3Count);
                return count == indexed3Count;
              } catch (MeterNotFoundException e) {
                return false;
              }
            });

    SearchMetadataStore verifyQuerySearchMetadataStore =
        new SearchMetadataStore(metadataStore, true);

    KaldbSearch.SearchResult searchResponse =
        queryServiceStub.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("*:*")
                .setStartTimeEpochMs(0L)
                .setEndTimeEpochMs(1601547099000L)
                .setHowMany(100)
                .setBucketCount(2)
                .build());

    assertThat(searchResponse.getTotalNodes()).isEqualTo(3);
    assertThat(searchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(searchResponse.getTotalCount()).isEqualTo(300);
    assertThat(searchResponse.getHitsCount()).isEqualTo(100);

    // close an indexer and make sure it's not searchable
    assertThat(verifyQuerySearchMetadataStore.list().get().size()).isEqualTo(3);
    indexingServiceManager3.stopAsync().awaitStopped(30, TimeUnit.SECONDS);
    await().until(() -> verifyQuerySearchMetadataStore.list().get().size() == 2);

    searchResponse =
        queryServiceStub.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("*:*")
                .setStartTimeEpochMs(0L)
                .setEndTimeEpochMs(1601547099000L)
                .setHowMany(100)
                .setBucketCount(2)
                .build());

    assertThat(searchResponse.getTotalNodes()).isEqualTo(2);
    assertThat(searchResponse.getFailedNodes()).isEqualTo(0);
    assertThat(searchResponse.getTotalCount()).isEqualTo(200);
    assertThat(searchResponse.getHitsCount()).isEqualTo(100);
  }

  private void testSearchWithOneShardTimeout() {
    KaldbDistributedQueryService.READ_TIMEOUT_MS = 2000;
    KaldbSearch.SearchResult searchResponse =
        queryServiceStub.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("*:*")
                .setStartTimeEpochMs(0L)
                .setEndTimeEpochMs(1601547099000L)
                .setHowMany(100)
                .setBucketCount(2)
                .build());

    assertThat(searchResponse.getTotalNodes()).isEqualTo(2);
    assertThat(searchResponse.getFailedNodes()).isEqualTo(1);
    assertThat(searchResponse.getTotalCount()).isEqualTo(100);
    assertThat(searchResponse.getHitsCount()).isEqualTo(100);
  }
}
