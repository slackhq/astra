package com.slack.kaldb.elasticsearchApi;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.linecorp.armeria.common.HttpStatus.OK;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.preprocessor.PreprocessorService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.OpenSearchBulkIngestAPI;
import com.slack.kaldb.testlib.MetricsUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.util.JsonUtil;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
    LOG.info("Calling shutdown()");
    kafkaServer.close();
    zkServer.close();
    meterRegistry.close();
  }

  @BeforeEach
  public void setUp() throws Exception {
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
            100,
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
        new OpenSearchBulkIngestAPI(datasetMetadataStore, preprocessorConfig, meterRegistry);

    openSearchBulkAPI.startAsync();
    openSearchBulkAPI.awaitRunning(DEFAULT_START_STOP_DURATION);
    assertThat(MetricsUtil.getTimerCount(PreprocessorService.CONFIG_RELOAD_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void testBulkApiEmpty() throws IOException {
    AggregatedHttpResponse response = openSearchBulkAPI.addDocument("{}\n").aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(INTERNAL_SERVER_ERROR.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    String request =
        """
            { "index": {"_index": "testindex", "_id": "1"} }
            { "field1" : "value1" }
            """;
    response = openSearchBulkAPI.addDocument(request).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(INTERNAL_SERVER_ERROR.code());
    responseObj = JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);
  }

  @Test
  public void testBulkApiBasic() throws IOException {
    String request =
        """
                { "index": {"_index": "testindex", "_id": "1"} }
                { "field1" : "value1" }
                """;
    AggregatedHttpResponse response = openSearchBulkAPI.addDocument(request).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(true);
    assertThat(response.status().code()).isEqualTo(OK.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(1);
    assertThat(responseObj.failedDocs()).isEqualTo(0);
  }
}
