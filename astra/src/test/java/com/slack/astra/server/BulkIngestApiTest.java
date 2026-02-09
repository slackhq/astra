package com.slack.astra.server;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.linecorp.armeria.common.HttpStatus.OK;
import static com.linecorp.armeria.common.HttpStatus.TOO_MANY_REQUESTS;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.slack.astra.bulkIngestApi.BulkIngestApi;
import com.slack.astra.bulkIngestApi.BulkIngestKafkaProducer;
import com.slack.astra.bulkIngestApi.BulkIngestResponse;
import com.slack.astra.bulkIngestApi.DatasetRateLimitingService;
import com.slack.astra.bulkIngestApi.DatasetSchemaService;
import com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.preprocessor.PreprocessorMetadataStore;
import com.slack.astra.metadata.schema.SchemaMetadata;
import com.slack.astra.metadata.schema.SchemaMetadataStore;
import com.slack.astra.preprocessor.PreprocessorRateLimiter;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.testlib.MetricsUtil;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import com.slack.astra.testlib.TestKafkaServer;
import com.slack.astra.util.JsonUtil;
import com.slack.astra.util.TestingZKServer;
import com.slack.service.murron.trace.Trace;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkIngestApiTest {
  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestApi.class);
  private static MeterRegistry meterRegistry;
  private static AsyncCuratorFramework curatorFramework;
  private static AstraConfigs.PreprocessorConfig preprocessorConfig;
  private static DatasetMetadataStore datasetMetadataStore;
  private static PreprocessorMetadataStore preprocessorMetadataStore;
  private static TestingServer zkServer;
  private static TestKafkaServer kafkaServer;
  private BulkIngestApi bulkApi;
  private static EtcdCluster etcdCluster;
  private static Client etcdClient;

  private BulkIngestKafkaProducer bulkIngestKafkaProducer;
  private DatasetRateLimitingService datasetRateLimitingService;
  private DatasetSchemaService datasetSchemaService;
  private static SchemaMetadataStore schemaMetadataStore;

  static String INDEX_NAME = "testindex";

  private static String DOWNSTREAM_TOPIC = "test-topic-out";

  @BeforeEach
  public void bootstrapCluster() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();

    zkServer = TestingZKServer.createTestingServer();
    etcdCluster = TestEtcdClusterFactory.start();

    // Create etcd client
    etcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .namespace(ByteSequence.from("testMetadata", java.nio.charset.StandardCharsets.UTF_8))
            .build();

    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("testMetadata")
            .setEnabled(true)
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
            .build();

    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("testMetadata")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .setZkCacheInitTimeoutMs(1000)
            .build();
    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);

    kafkaServer = new TestKafkaServer();
    kafkaServer.createTopicWithPartitions(DOWNSTREAM_TOPIC, 3);

    AstraConfigs.ServerConfig serverConfig =
        AstraConfigs.ServerConfig.newBuilder()
            .setServerPort(8080)
            .setServerAddress("localhost")
            .build();
    AstraConfigs.KafkaConfig kafkaConfig =
        AstraConfigs.KafkaConfig.newBuilder()
            .setKafkaBootStrapServers(kafkaServer.getBroker().getBrokerList().get())
            .setKafkaTopic(DOWNSTREAM_TOPIC)
            .build();
    preprocessorConfig =
        AstraConfigs.PreprocessorConfig.newBuilder()
            .setKafkaConfig(kafkaConfig)
            .setServerConfig(serverConfig)
            .setPreprocessorInstanceCount(1)
            .setRateLimiterMaxBurstSeconds(1)
            .setDatasetRateLimitAggregationSecs(1)
            .setDatasetRateLimitPeriodSecs(15)
            .build();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .putStoreModes("DatasetMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SnapshotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("ReplicaMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("HpaMetricMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SearchMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheSlotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeAssignmentStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes(
                "FieldRedactionMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("PreprocessorMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryTaskMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SchemaMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .setZookeeperConfig(zkConfig)
            .setEtcdConfig(etcdConfig)
            .build();

    datasetMetadataStore =
        new DatasetMetadataStore(
            curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true);
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            INDEX_NAME,
            "owner",
            1,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0"))),
            INDEX_NAME);

    // Create an entry while init. Update the entry on every test run
    datasetMetadataStore.createSync(datasetMetadata);

    preprocessorMetadataStore =
        new PreprocessorMetadataStore(
            curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true);
    schemaMetadataStore =
        new SchemaMetadataStore(
            curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, true);

    datasetRateLimitingService =
        new DatasetRateLimitingService(
            datasetMetadataStore, preprocessorMetadataStore, preprocessorConfig, meterRegistry);

    datasetRateLimitingService.startAsync();
    datasetRateLimitingService.awaitRunning(DEFAULT_START_STOP_DURATION);

    datasetSchemaService = new DatasetSchemaService(schemaMetadataStore);
    datasetSchemaService.startAsync();
    datasetSchemaService.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  // I looked at making this a @BeforeEach. it's possible if you annotate a test with a @Tag and
  // pass throughputBytes.
  // However, decided not to go with that because it involved hardcoding the throughput bytes
  // when defining the test. We need it to be dynamic based on the size of the docs
  public void updateDatasetThroughput(int throughputBytes) {
    double timerCount =
        MetricsUtil.getTimerCount(
            DatasetRateLimitingService.RATE_LIMIT_RELOAD_TIMER, meterRegistry);

    // dataset metadata already exists. Update with the throughput value
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            INDEX_NAME,
            "owner",
            throughputBytes,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0"))),
            INDEX_NAME);
    datasetMetadataStore.updateSync(datasetMetadata);

    // Need to wait until the rate limit has been loaded
    await()
        .until(
            () ->
                MetricsUtil.getTimerCount(
                        DatasetRateLimitingService.RATE_LIMIT_RELOAD_TIMER, meterRegistry)
                    > timerCount);
  }

  @AfterEach
  // Every test manually calls setup() since we need to pass a param while creating the dataset
  // Instead of calling stop from every test and ensuring it's part of a finally block we just call
  // the shutdown code with the @AfterEach annotation
  public void shutdownOpenSearchAPI() throws Exception {
    System.clearProperty("astra.bulkIngest.useKafkaTransactions");
    System.clearProperty(BulkIngestApi.ASTRA_SCHEMA_ENFORCEMENT_FLAG);
    if (datasetRateLimitingService != null) {
      datasetRateLimitingService.stopAsync();
      datasetRateLimitingService.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (datasetSchemaService != null) {
      datasetSchemaService.stopAsync();
      datasetSchemaService.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (bulkIngestKafkaProducer != null) {
      bulkIngestKafkaProducer.stopAsync();
      bulkIngestKafkaProducer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    kafkaServer.close();

    datasetMetadataStore.close();
    preprocessorMetadataStore.close();
    schemaMetadataStore.close();
    curatorFramework.unwrap().close();
    zkServer.close();
    meterRegistry.close();
    if (etcdClient != null) {
      etcdClient.close();
    }
  }

  public KafkaConsumer getTestKafkaConsumer() {
    // used to verify the message exist on the downstream topic
    Properties properties = kafkaServer.getBroker().consumerConfig();
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
    properties.put("isolation.level", "read_committed");
    KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
    kafkaConsumer.subscribe(List.of(DOWNSTREAM_TOPIC));
    return kafkaConsumer;
  }

  @Test
  public void testBulkApiBasic() throws Exception {
    bulkIngestKafkaProducer =
        new BulkIngestKafkaProducer(datasetMetadataStore, preprocessorConfig, meterRegistry);
    bulkIngestKafkaProducer.startAsync();
    bulkIngestKafkaProducer.awaitRunning(DEFAULT_START_STOP_DURATION);
    bulkApi =
        new BulkIngestApi(
            bulkIngestKafkaProducer,
            datasetRateLimitingService,
            meterRegistry,
            400,
            Schema.IngestSchema.getDefaultInstance());

    String request1 =
        """
            { "index": {"_index": "testindex", "_id": "1"} }
            { "field1" : "value1" }
            """;
    // use the way we calculate the throughput in the rate limiter to get the exact bytes
    Map<String, List<Trace.Span>> docs =
        BulkApiRequestParser.parseRequest(
            request1.getBytes(StandardCharsets.UTF_8), Schema.IngestSchema.newBuilder().build());
    int limit = PreprocessorRateLimiter.getSpanBytes(docs.get("testindex"));
    // for some reason if we pass the exact limit, the rate limiter doesn't work as expected
    updateDatasetThroughput(limit / 2);

    // test with empty causes a parse exception
    AggregatedHttpResponse response = bulkApi.addDocument("{}\n").aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(INTERNAL_SERVER_ERROR.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    // test with request1 twice. first one should succeed, second one will fail because of rate
    // limiter
    AggregatedHttpResponse httpResponse = bulkApi.addDocument(request1).aggregate().join();
    assertThat(httpResponse.status().isSuccess()).isEqualTo(true);
    assertThat(httpResponse.status().code()).isEqualTo(OK.code());
    try {
      BulkIngestResponse httpResponseObj =
          JsonUtil.read(httpResponse.contentUtf8(), BulkIngestResponse.class);
      assertThat(httpResponseObj.totalDocs()).isEqualTo(1);
      assertThat(httpResponseObj.failedDocs()).isEqualTo(0);
    } catch (IOException e) {
      fail("", e);
    }

    httpResponse = bulkApi.addDocument(request1).aggregate().join();
    assertThat(httpResponse.status().isSuccess()).isEqualTo(false);
    assertThat(httpResponse.status().code()).isEqualTo(400);
    try {
      BulkIngestResponse httpResponseObj =
          JsonUtil.read(httpResponse.contentUtf8(), BulkIngestResponse.class);
      assertThat(httpResponseObj.totalDocs()).isEqualTo(0);
      assertThat(httpResponseObj.failedDocs()).isEqualTo(0);
      assertThat(httpResponseObj.errorMsg()).isEqualTo("rate limit exceeded");
    } catch (IOException e) {
      fail("", e);
    }

    // test with multiple indexes
    String request2 =
        """
            { "index": {"_index": "testindex1", "_id": "1"} }
            { "field1" : "value1" }
            { "index": {"_index": "testindex2", "_id": "1"} }
            { "field1" : "value1" }
            """;
    response = bulkApi.addDocument(request2).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(INTERNAL_SERVER_ERROR.code());
    responseObj = JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);
    assertThat(responseObj.errorMsg()).isEqualTo("request must contain only 1 unique index");

    BulkIngestApi bulkApi2 =
        new BulkIngestApi(
            bulkIngestKafkaProducer,
            datasetRateLimitingService,
            meterRegistry,
            TOO_MANY_REQUESTS.code(),
            Schema.IngestSchema.getDefaultInstance());
    httpResponse = bulkApi2.addDocument(request1).aggregate().join();
    assertThat(httpResponse.status().isSuccess()).isEqualTo(false);
    assertThat(httpResponse.status().code()).isEqualTo(TOO_MANY_REQUESTS.code());
    try {
      BulkIngestResponse httpResponseObj =
          JsonUtil.read(httpResponse.contentUtf8(), BulkIngestResponse.class);
      assertThat(httpResponseObj.totalDocs()).isEqualTo(0);
      assertThat(httpResponseObj.failedDocs()).isEqualTo(0);
      assertThat(httpResponseObj.errorMsg()).isEqualTo("rate limit exceeded");
    } catch (IOException e) {
      fail("", e);
    }
  }

  @Test
  public void testDocumentInKafkaWithTransactionsSimple() throws Exception {
    System.setProperty("astra.bulkIngest.useKafkaTransactions", "true");
    bulkIngestKafkaProducer =
        new BulkIngestKafkaProducer(datasetMetadataStore, preprocessorConfig, meterRegistry);
    bulkIngestKafkaProducer.startAsync();
    bulkIngestKafkaProducer.awaitRunning(DEFAULT_START_STOP_DURATION);
    bulkApi =
        new BulkIngestApi(
            bulkIngestKafkaProducer,
            datasetRateLimitingService,
            meterRegistry,
            400,
            Schema.IngestSchema.getDefaultInstance());

    String request1 =
        """
            { "index": {"_index": "testindex", "_id": "1"} }
            { "field1" : "value1" },
            { "index": {"_index": "testindex", "_id": "2"} }
            { "field1" : "value2" }
            """;
    updateDatasetThroughput(request1.getBytes(StandardCharsets.UTF_8).length);

    KafkaConsumer kafkaConsumer = getTestKafkaConsumer();

    AggregatedHttpResponse response = bulkApi.addDocument(request1).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(true);
    assertThat(response.status().code()).isEqualTo(OK.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(2);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    // kafka transaction adds a "control batch" record at the end of the transaction so the offset
    // will always be n+1
    validateOffset(kafkaConsumer, 3);

    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(2);
    assertThat(records)
        .anyMatch(
            record ->
                TraceSpanParserSilenceError(record.value()).getId().toStringUtf8().equals("1"));
    assertThat(records)
        .anyMatch(
            record ->
                TraceSpanParserSilenceError(record.value()).getId().toStringUtf8().equals("2"));

    // close the kafka consumer used in the test
    kafkaConsumer.close();
  }

  @Test
  public void testDocumentInKafkaWithoutTransactionsSimple() throws Exception {
    System.setProperty("astra.bulkIngest.useKafkaTransactions", "false");
    bulkIngestKafkaProducer =
        new BulkIngestKafkaProducer(datasetMetadataStore, preprocessorConfig, meterRegistry);
    bulkIngestKafkaProducer.startAsync();
    bulkIngestKafkaProducer.awaitRunning(DEFAULT_START_STOP_DURATION);
    bulkApi =
        new BulkIngestApi(
            bulkIngestKafkaProducer,
            datasetRateLimitingService,
            meterRegistry,
            400,
            Schema.IngestSchema.getDefaultInstance());

    String request1 =
        """
            { "index": {"_index": "testindex", "_id": "1"} }
            { "field1" : "value1" },
            { "index": {"_index": "testindex", "_id": "2"} }
            { "field1" : "value2" }
            """;
    updateDatasetThroughput(request1.getBytes(StandardCharsets.UTF_8).length);

    KafkaConsumer kafkaConsumer = getTestKafkaConsumer();

    AggregatedHttpResponse response = bulkApi.addDocument(request1).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(true);
    assertThat(response.status().code()).isEqualTo(OK.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(2);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    validateOffset(kafkaConsumer, 2);

    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(2);
    assertThat(records)
        .anyMatch(
            record ->
                TraceSpanParserSilenceError(record.value()).getId().toStringUtf8().equals("1"));
    assertThat(records)
        .anyMatch(
            record ->
                TraceSpanParserSilenceError(record.value()).getId().toStringUtf8().equals("2"));

    // close the kafka consumer used in the test
    kafkaConsumer.close();
  }

  // Validate that schema is used during ingest (that filtering happens)
  @Test
  public void testStaticSchemaMode() throws Exception {
    System.setProperty(BulkIngestApi.ASTRA_SCHEMA_ENFORCEMENT_FLAG, "true");

    // Write a schema with DROP_UNKNOWN mode to the store — only "field1" is allowed
    Schema.IngestSchema schema =
        Schema.IngestSchema.newBuilder()
            .putFields(
                "field1",
                Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.TEXT).build())
            .build();
    SchemaMetadata schemaMeta =
        new SchemaMetadata(
            SchemaMetadata.GLOBAL_SCHEMA_NAME,
            schema,
            AstraConfigs.SchemaMode.SCHEMA_MODE_DROP_UNKNOWN);
    schemaMetadataStore.createSync(schemaMeta);

    // Restart the schema service so it picks up the new schema
    datasetSchemaService.stopAsync();
    datasetSchemaService.awaitTerminated(DEFAULT_START_STOP_DURATION);
    datasetSchemaService = new DatasetSchemaService(schemaMetadataStore);
    datasetSchemaService.startAsync();
    datasetSchemaService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(datasetSchemaService.getSchemaMode())
        .isEqualTo(AstraConfigs.SchemaMode.SCHEMA_MODE_DROP_UNKNOWN);

    bulkIngestKafkaProducer =
        new BulkIngestKafkaProducer(datasetMetadataStore, preprocessorConfig, meterRegistry);
    bulkIngestKafkaProducer.startAsync();
    bulkIngestKafkaProducer.awaitRunning(DEFAULT_START_STOP_DURATION);
    bulkApi =
        new BulkIngestApi(
            bulkIngestKafkaProducer,
            datasetRateLimitingService,
            meterRegistry,
            400,
            datasetSchemaService);

    // Send a document with field1 (in schema) and field2 (not in schema)
    String request =
        """
            { "index": {"_index": "testindex", "_id": "1"} }
            { "field1" : "value1", "field2" : "value2" }
            """;
    updateDatasetThroughput(request.getBytes(StandardCharsets.UTF_8).length);

    KafkaConsumer kafkaConsumer = getTestKafkaConsumer();

    AggregatedHttpResponse response = bulkApi.addDocument(request).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(true);
    assertThat(response.status().code()).isEqualTo(OK.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(1);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    validateOffset(kafkaConsumer, 1);

    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
    assertThat(records.count()).isEqualTo(1);

    // Verify field2 was dropped — only field1 should remain (service_name is also not in schema)
    Trace.Span span = TraceSpanParserSilenceError(records.iterator().next().value());
    List<String> tagKeys = span.getTagsList().stream().map(Trace.KeyValue::getKey).toList();
    assertThat(tagKeys).contains("field1");
    assertThat(tagKeys).doesNotContain("field2", "service_name");

    kafkaConsumer.close();
  }

  // Validate schema mode set to dynamic does not filter out fields
  @Test
  public void testDynamicSchemaMode() throws Exception {
    // Default preprocessorConfig has no schema file, so mode is DYNAMIC
    assertThat(datasetSchemaService.getSchemaMode())
        .isEqualTo(AstraConfigs.SchemaMode.SCHEMA_MODE_DYNAMIC);

    bulkIngestKafkaProducer =
        new BulkIngestKafkaProducer(datasetMetadataStore, preprocessorConfig, meterRegistry);
    bulkIngestKafkaProducer.startAsync();
    bulkIngestKafkaProducer.awaitRunning(DEFAULT_START_STOP_DURATION);
    bulkApi =
        new BulkIngestApi(
            bulkIngestKafkaProducer,
            datasetRateLimitingService,
            meterRegistry,
            400,
            Schema.IngestSchema.getDefaultInstance());

    String request =
        """
            { "index": {"_index": "testindex", "_id": "1"} }
            { "field1" : "value1", "field2" : "value2", "field3" : "value3" }
            """;
    updateDatasetThroughput(request.getBytes(StandardCharsets.UTF_8).length);

    KafkaConsumer kafkaConsumer = getTestKafkaConsumer();

    AggregatedHttpResponse response = bulkApi.addDocument(request).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(true);
    assertThat(response.status().code()).isEqualTo(OK.code());

    validateOffset(kafkaConsumer, 1);

    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
    assertThat(records.count()).isEqualTo(1);

    // All fields should be preserved in dynamic mode
    Trace.Span span = TraceSpanParserSilenceError(records.iterator().next().value());
    List<String> tagKeys = span.getTagsList().stream().map(Trace.KeyValue::getKey).toList();
    assertThat(tagKeys).contains("field1", "field2", "field3");

    kafkaConsumer.close();
  }

  // Validate filterDocsBySchemaMode drops unknown fields in DROP_UNKNOWN mode
  @Test
  public void testFilterDocsBySchemaModeStatic() {
    Schema.IngestSchema schema =
        Schema.IngestSchema.newBuilder()
            .putFields(
                "allowed",
                Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.TEXT).build())
            .build();

    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey("allowed").setVStr("val1").build())
            .addTags(Trace.KeyValue.newBuilder().setKey("unknown").setVStr("val2").build())
            .addTags(Trace.KeyValue.newBuilder().setKey("service_name").setVStr("svc").build())
            .build();

    Map<String, List<Trace.Span>> docs = Map.of("testindex", List.of(span));
    Map<String, List<Trace.Span>> filtered =
        BulkIngestApi.filterDocsBySchemaMode(
            docs, schema, AstraConfigs.SchemaMode.SCHEMA_MODE_DROP_UNKNOWN);

    List<String> tagKeys =
        filtered.get("testindex").getFirst().getTagsList().stream()
            .map(Trace.KeyValue::getKey)
            .toList();
    assertThat(tagKeys).containsExactly("allowed");
    assertThat(tagKeys).doesNotContain("unknown", "service_name");
  }

  // Validate filterDocsBySchemaMode is a no-op in DYNAMIC mode
  @Test
  public void testFilterDocsBySchemaModeDynamic() {
    Schema.IngestSchema schema =
        Schema.IngestSchema.newBuilder()
            .putFields(
                "allowed",
                Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.TEXT).build())
            .build();

    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey("allowed").setVStr("val1").build())
            .addTags(Trace.KeyValue.newBuilder().setKey("unknown").setVStr("val2").build())
            .build();

    Map<String, List<Trace.Span>> docs = Map.of("testindex", List.of(span));
    Map<String, List<Trace.Span>> filtered =
        BulkIngestApi.filterDocsBySchemaMode(
            docs, schema, AstraConfigs.SchemaMode.SCHEMA_MODE_DYNAMIC);

    // Should return docs unchanged — same reference
    assertThat(filtered).isSameAs(docs);
  }

  // Validate filterSpanTags keeps schema fields, service_name, and multi-field subfields
  @Test
  public void testFilterSpanTags() {
    Schema.IngestSchema schema =
        Schema.IngestSchema.newBuilder()
            .putFields(
                "status",
                Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.KEYWORD).build())
            .putFields(
                "message",
                Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.TEXT).build())
            .build();

    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey("status").setVStr("200").build())
            .addTags(Trace.KeyValue.newBuilder().setKey("status.keyword").setVStr("200").build())
            .addTags(Trace.KeyValue.newBuilder().setKey("message").setVStr("ok").build())
            .addTags(Trace.KeyValue.newBuilder().setKey("service_name").setVStr("svc").build())
            .addTags(Trace.KeyValue.newBuilder().setKey("unknown_field").setVStr("drop").build())
            .addTags(
                Trace.KeyValue.newBuilder().setKey("unknown_field.keyword").setVStr("drop").build())
            .build();

    Trace.Span filtered = BulkIngestApi.filterSpanTags(span, schema);
    List<String> tagKeys = filtered.getTagsList().stream().map(Trace.KeyValue::getKey).toList();
    assertThat(tagKeys).containsExactlyInAnyOrder("status", "status.keyword", "message");
    assertThat(tagKeys).doesNotContain("unknown_field", "unknown_field.keyword", "service_name");
  }

  // Validate filterDocsBySchemaMode drops all fields when none match schema, leaving only
  // service_name
  @Test
  public void testFilterDocsBySchemaModeAllFieldsUnknown() {
    Schema.IngestSchema schema =
        Schema.IngestSchema.newBuilder()
            .putFields(
                "allowed_field",
                Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.TEXT).build())
            .build();

    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey("unknown1").setVStr("val1").build())
            .addTags(Trace.KeyValue.newBuilder().setKey("unknown2").setVStr("val2").build())
            .addTags(Trace.KeyValue.newBuilder().setKey("service_name").setVStr("svc").build())
            .build();

    Map<String, List<Trace.Span>> docs = Map.of("testindex", List.of(span));
    Map<String, List<Trace.Span>> filtered =
        BulkIngestApi.filterDocsBySchemaMode(
            docs, schema, AstraConfigs.SchemaMode.SCHEMA_MODE_DROP_UNKNOWN);

    Trace.Span filteredSpan = filtered.get("testindex").getFirst();
    List<String> tagKeys = filteredSpan.getTagsList().stream().map(Trace.KeyValue::getKey).toList();
    // All fields were unknown — span has no tags
    assertThat(tagKeys).isEmpty();
  }

  // Validate filterSpanTags with empty schema only keeps service_name
  @Test
  public void testFilterSpanTagsEmptySchema() {
    Schema.IngestSchema schema = Schema.IngestSchema.getDefaultInstance();

    Trace.Span span =
        Trace.Span.newBuilder()
            .addTags(Trace.KeyValue.newBuilder().setKey("field1").setVStr("val1").build())
            .addTags(Trace.KeyValue.newBuilder().setKey("service_name").setVStr("svc").build())
            .build();

    Trace.Span filtered = BulkIngestApi.filterSpanTags(span, schema);
    List<String> tagKeys = filtered.getTagsList().stream().map(Trace.KeyValue::getKey).toList();
    assertThat(tagKeys).isEmpty();
  }

  // Validate DatasetSchemaService defaults when store is empty
  @Test
  public void testSchemaServiceDefaultsWithEmptyStore() {
    assertThat(datasetSchemaService.getSchemaMode())
        .isEqualTo(AstraConfigs.SchemaMode.SCHEMA_MODE_DYNAMIC);
    // Default schema should still contain reserved fields (timestamp, message, _all, etc.)
    assertThat(datasetSchemaService.getSchema().getFieldsCount()).isGreaterThan(0);
    assertThat(datasetSchemaService.getSchema().containsFields("@timestamp")).isTrue();
  }

  // Validate DatasetSchemaService loads schema from store
  @Test
  public void testSchemaServiceLoadsFromStore() throws Exception {
    Schema.IngestSchema schema =
        Schema.IngestSchema.newBuilder()
            .putFields(
                "my_field",
                Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.KEYWORD).build())
            .putFields(
                "another_field",
                Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.INTEGER).build())
            .build();
    schemaMetadataStore.createSync(
        new SchemaMetadata(
            SchemaMetadata.GLOBAL_SCHEMA_NAME,
            schema,
            AstraConfigs.SchemaMode.SCHEMA_MODE_DROP_UNKNOWN));

    datasetSchemaService.stopAsync();
    datasetSchemaService.awaitTerminated(DEFAULT_START_STOP_DURATION);
    datasetSchemaService = new DatasetSchemaService(schemaMetadataStore);
    datasetSchemaService.startAsync();
    datasetSchemaService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(datasetSchemaService.getSchemaMode())
        .isEqualTo(AstraConfigs.SchemaMode.SCHEMA_MODE_DROP_UNKNOWN);
    assertThat(datasetSchemaService.getSchema().containsFields("my_field")).isTrue();
    assertThat(datasetSchemaService.getSchema().containsFields("another_field")).isTrue();
    // Reserved fields should also be present
    assertThat(datasetSchemaService.getSchema().containsFields("@timestamp")).isTrue();
  }

  public void validateOffset(KafkaConsumer kafkaConsumer, long expectedOffset) {
    await()
        .until(
            () -> {
              @SuppressWarnings("OptionalGetWithoutIsPresent")
              long partitionOffset =
                  (Long)
                      kafkaConsumer
                          .endOffsets(List.of(new TopicPartition(DOWNSTREAM_TOPIC, 0)))
                          .values()
                          .stream()
                          .findFirst()
                          .get();
              LOG.debug(
                  "Current partitionOffset - {}. expecting offset to be - {}",
                  partitionOffset,
                  expectedOffset);
              return partitionOffset == expectedOffset;
            });
  }

  private static Trace.Span TraceSpanParserSilenceError(byte[] data) {
    try {
      return Trace.Span.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      return Trace.Span.newBuilder().build();
    }
  }
}
