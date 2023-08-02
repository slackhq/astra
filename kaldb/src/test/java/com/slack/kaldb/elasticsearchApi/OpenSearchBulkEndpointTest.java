package com.slack.kaldb.elasticsearchApi;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.linecorp.armeria.common.HttpStatus.OK;
import static com.linecorp.armeria.common.HttpStatus.TOO_MANY_REQUESTS;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.preprocessor.PreprocessorRateLimiter;
import com.slack.kaldb.preprocessor.PreprocessorService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.OpenSearchBulkIngestAPI;
import com.slack.kaldb.testlib.MetricsUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.util.JsonUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.List;
import java.util.Map;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchBulkEndpointTest {

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchBulkEndpointTest.class);

  private PrometheusMeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;
  private DatasetMetadataStore datasetMetadataStore;
  private TestingServer zkServer;
  private TestKafkaServer kafkaServer;
  private OpenSearchBulkIngestAPI openSearchBulkAPI;

  String INDEX_NAME = "testindex";

  @BeforeAll
  public static void beforeClass() {
    Tracing.newBuilder().build();
  }

  @AfterEach
  public void shutdown() throws Exception {
    kafkaServer.close();
    zkServer.close();
    meterRegistry.close();
  }

  public void setUp(int throughputBytes) throws Exception {
    meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    zkServer = new TestingServer();
    kafkaServer = new TestKafkaServer();

    // initialize the downstream topic
    String downstreamTopic = "test-topic-out";
    kafkaServer.createTopicWithPartitions(downstreamTopic, 3);

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("testZK")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();
    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    datasetMetadataStore = new DatasetMetadataStore(curatorFramework, true);

    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            INDEX_NAME,
            "owner",
            throughputBytes,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0", "1", "2"))),
            INDEX_NAME);
    datasetMetadataStore.createSync(datasetMetadata);

    KaldbConfigs.ServerConfig serverConfig =
        KaldbConfigs.ServerConfig.newBuilder()
            .setServerPort(8080)
            .setServerAddress("localhost")
            .build();
    KaldbConfigs.PreprocessorConfig preprocessorConfig =
        KaldbConfigs.PreprocessorConfig.newBuilder()
            .setBootstrapServers(kafkaServer.getBroker().getBrokerList().get())
            .setServerConfig(serverConfig)
            .setPreprocessorInstanceCount(1)
            .setRateLimiterMaxBurstSeconds(1)
            .setDownstreamTopic(downstreamTopic)
            .build();
    openSearchBulkAPI =
        new OpenSearchBulkIngestAPI(datasetMetadataStore, preprocessorConfig, meterRegistry, false);

    openSearchBulkAPI.startAsync();
    openSearchBulkAPI.awaitRunning(DEFAULT_START_STOP_DURATION);
    assertThat(MetricsUtil.getTimerCount(PreprocessorService.CONFIG_RELOAD_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void testBulkApiBasic() throws Exception {
    String request1 =
        """
                { "index": {"_index": "testindex", "_id": "1"} }
                { "field1" : "value1" }
                """;
    List<IndexRequest> indexRequests = OpenSearchRequest.parseBulkRequest(request1);
    Map<String, List<Trace.Span>> indexDocs =
        OpenSearchRequest.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("testindex").size()).isEqualTo(1);
    int throughputBytes = PreprocessorRateLimiter.getSpanBytes(indexDocs.get("testindex"));

    // we create a preprocessorconfig with num_preprocessors=1
    setUp(throughputBytes);

    // test with empty causes a parse exception
    AggregatedHttpResponse response = openSearchBulkAPI.addDocument("{}\n").aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(INTERNAL_SERVER_ERROR.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    // test with request1 twice. first one should succeed and second one will fail because of rate
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
}
