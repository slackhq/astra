package com.slack.kaldb.elasticsearchApi;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.linecorp.armeria.common.HttpStatus.OK;
import static com.linecorp.armeria.common.HttpStatus.TOO_MANY_REQUESTS;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.preprocessor.PreprocessorRateLimiter;
import com.slack.kaldb.preprocessor.ingest.OpenSearchBulkApiRequestParser;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.OpenSearchBulkIngestApi;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.util.JsonUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
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
import org.opensearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchBulkEndpointTest {

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchBulkEndpointTest.class);

  private static PrometheusMeterRegistry meterRegistry;
  private static AsyncCuratorFramework curatorFramework;
  private static KaldbConfigs.PreprocessorConfig preprocessorConfig;
  private static DatasetMetadataStore datasetMetadataStore;
  private static TestingServer zkServer;
  private static TestKafkaServer kafkaServer;
  private OpenSearchBulkIngestApi openSearchBulkAPI;

  static String INDEX_NAME = "testindex";

  private static String DOWNSTREAM_TOPIC = "test-topic-out";

  @BeforeEach
  public void bootstrapCluster() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    zkServer = new TestingServer();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("testZK")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();
    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);

    kafkaServer = new TestKafkaServer();
    kafkaServer.createTopicWithPartitions(DOWNSTREAM_TOPIC, 3);

    KaldbConfigs.ServerConfig serverConfig =
        KaldbConfigs.ServerConfig.newBuilder()
            .setServerPort(8080)
            .setServerAddress("localhost")
            .build();
    preprocessorConfig =
        KaldbConfigs.PreprocessorConfig.newBuilder()
            .setBootstrapServers(kafkaServer.getBroker().getBrokerList().get())
            .setUseBulkApi(true)
            .setServerConfig(serverConfig)
            .setPreprocessorInstanceCount(1)
            .setRateLimiterMaxBurstSeconds(1)
            .setDownstreamTopic(DOWNSTREAM_TOPIC)
            .build();

    datasetMetadataStore = new DatasetMetadataStore(curatorFramework, true);
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            INDEX_NAME,
            "owner",
            1,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0"))),
            INDEX_NAME);
    // Create an entry while init. Update the entry on every test run
    datasetMetadataStore.createSync(datasetMetadata);

    openSearchBulkAPI =
        new OpenSearchBulkIngestApi(datasetMetadataStore, preprocessorConfig, meterRegistry, false);

    openSearchBulkAPI.startAsync();
    openSearchBulkAPI.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  // I looked at making this a @BeforeEach. it's possible if you annotate a test with a @Tag and
  // pass throughputBytes.
  // However, decided not to go with that because it involved hardcoding the throughput bytes
  // when defining the test. We need it to be dynamic based on the size of the docs
  public void updateDatasetThroughput(int throughputBytes) throws Exception {
    // dataset metadata already exists. Update with the throughput value
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            INDEX_NAME,
            "owner",
            throughputBytes,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0"))),
            INDEX_NAME);
    datasetMetadataStore.updateSync(datasetMetadata);
  }

  @AfterEach
  // Every test manually calls setup() since we need to pass a param while creating the dataset
  // Instead of calling stop from every test and ensuring it's part of a finally block we just call
  // the shutdown code with the @AfterEach annotation
  public void shutdownOpenSearchAPI() throws Exception {
    if (openSearchBulkAPI != null) {
      openSearchBulkAPI.stopAsync();
      openSearchBulkAPI.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    kafkaServer.close();
    curatorFramework.unwrap().close();
    zkServer.close();
    meterRegistry.close();
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
    String request1 =
        """
                { "index": {"_index": "testindex", "_id": "1"} }
                { "field1" : "value1" }
                """;
    // get num bytes that can be used to create the dataset. When we make 2 successive calls the
    // second one should fail
    List<IndexRequest> indexRequests = OpenSearchBulkApiRequestParser.parseBulkRequest(request1);
    Map<String, List<Trace.Span>> indexDocs =
        OpenSearchBulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("testindex").size()).isEqualTo(1);
    assertThat(indexDocs.get("testindex").get(0).getId().toStringUtf8()).isEqualTo("1");
    int throughputBytes = PreprocessorRateLimiter.getSpanBytes(indexDocs.get("testindex"));
    updateDatasetThroughput(throughputBytes);

    // test with empty causes a parse exception
    AggregatedHttpResponse response = openSearchBulkAPI.addDocument("{}\n").aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(INTERNAL_SERVER_ERROR.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    // test with request1 twice. first one should succeed, second one will fail because of rate
    // limiter
    response = openSearchBulkAPI.addDocument(request1).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(true);
    assertThat(response.status().code()).isEqualTo(OK.code());
    responseObj = JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(1);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    response = openSearchBulkAPI.addDocument(request1).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(TOO_MANY_REQUESTS.code());
    responseObj = JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);
    assertThat(responseObj.errorMsg()).isEqualTo("rate limit exceeded");

    // test with multiple indexes
    String request2 =
        """
                { "index": {"_index": "testindex1", "_id": "1"} }
                { "field1" : "value1" }
                { "index": {"_index": "testindex2", "_id": "1"} }
                { "field1" : "value1" }
                """;
    response = openSearchBulkAPI.addDocument(request2).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(INTERNAL_SERVER_ERROR.code());
    responseObj = JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);
    assertThat(responseObj.errorMsg()).isEqualTo("request must contain only 1 unique index");
  }

  @Test
  public void testDocumentInKafkaSimple() throws Exception {
    String request1 =
        """
                    { "index": {"_index": "testindex", "_id": "1"} }
                    { "field1" : "value1" },
                    { "index": {"_index": "testindex", "_id": "2"} }
                    { "field1" : "value2" }
                    """;
    List<IndexRequest> indexRequests = OpenSearchBulkApiRequestParser.parseBulkRequest(request1);
    Map<String, List<Trace.Span>> indexDocs =
        OpenSearchBulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("testindex").size()).isEqualTo(2);
    int throughputBytes = PreprocessorRateLimiter.getSpanBytes(indexDocs.get("testindex"));
    updateDatasetThroughput(throughputBytes);

    KafkaConsumer kafkaConsumer = getTestKafkaConsumer();

    AggregatedHttpResponse response = openSearchBulkAPI.addDocument(request1).aggregate().join();
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
  public void testDocumentInKafkaTransactionError() throws Exception {
    updateDatasetThroughput(100_1000);

    KafkaConsumer kafkaConsumer = getTestKafkaConsumer();

    // we want to inject a failure in the second doc and test if the abort transaction works and we
    // don't index the first document
    Trace.Span doc1 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("error1")).build();
    Trace.Span doc2 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("error2")).build();
    Trace.Span doc3 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("error3")).build();
    Trace.Span doc4 = spy(Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("error4")).build());
    when(doc4.toByteArray()).thenThrow(new RuntimeException("exception"));
    Trace.Span doc5 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("error5")).build();

    Map<String, List<Trace.Span>> indexDocs =
        Map.of("testindex", List.of(doc1, doc2, doc3, doc4, doc5));

    BulkIngestResponse responseObj = openSearchBulkAPI.produceDocuments(indexDocs);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(5);
    assertThat(responseObj.errorMsg()).isNotNull();

    // 1 for aborted txn?
    validateOffset(kafkaConsumer, 1);
    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(0);

    Trace.Span doc6 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("no_error6")).build();
    Trace.Span doc7 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("no_error7")).build();
    Trace.Span doc8 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("no_error8")).build();
    Trace.Span doc9 =
        spy(Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("no_error9")).build());
    Trace.Span doc10 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("no_error10")).build();

    indexDocs = Map.of("testindex", List.of(doc6, doc7, doc8, doc9, doc10));

    responseObj = openSearchBulkAPI.produceDocuments(indexDocs);
    assertThat(responseObj.totalDocs()).isEqualTo(5);
    assertThat(responseObj.failedDocs()).isEqualTo(0);
    assertThat(responseObj.errorMsg()).isNotNull();

    // 5 docs. 1 control batch. initial offset was 1 after the first failed batch
    validateOffset(kafkaConsumer, 7);
    records = kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(5);
    records.forEach(
        record ->
            LOG.info(
                "Trace= + " + TraceSpanParserSilenceError(record.value()).getId().toStringUtf8()));

    // close the kafka consumer used in the test
    kafkaConsumer.close();
  }

  public void validateOffset(KafkaConsumer kafkaConsumer, int offset) {
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
                  offset);
              return partitionOffset == offset;
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
