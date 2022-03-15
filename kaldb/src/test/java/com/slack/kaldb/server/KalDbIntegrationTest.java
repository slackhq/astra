package com.slack.kaldb.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KalDbIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(KalDbIntegrationTest.class);

  private Kaldb kaldb;
  private TestingServer testingServer;
  private EphemeralKafkaBroker broker;
  private final ObjectMapper om = new ObjectMapper();
  private KaldbConfigs.KaldbConfig kaldbConfig;

  @Before
  public void setUp() throws Exception {
    testingServer = new TestingServer(2181);
    broker = EphemeralKafkaBroker.create(9092);
    broker.start().get(10, TimeUnit.SECONDS);

    kaldbConfig = KaldbConfig.fromYamlConfig(Files.readString(Path.of("../config/config.yaml")));
    kaldb = new Kaldb(kaldbConfig);
    LOG.info("Starting kalDb with the resolved configs: {}", kaldbConfig.toString());
    kaldb.start();
    kaldb.serviceManager.awaitHealthy();
  }

  @After
  public void tearDown() throws IOException, ExecutionException, InterruptedException {
    kaldb.shutdown();
    testingServer.close();
    broker.stop();
    kaldbConfig = null;
  }

  private String getResponse(String url) {
    try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
      HttpGet httpGet = new HttpGet(url);
      try (CloseableHttpResponse httpResponse = httpclient.execute(httpGet)) {
        HttpEntity entity = httpResponse.getEntity();

        String response = EntityUtils.toString(entity);
        EntityUtils.consume(entity);
        return response;
      }
    } catch (IOException e) {
      return null;
    }
  }

  private String getResponse(int port) {
    String url = String.format("http://localhost:%s/health", port);
    return getResponse(url);
  }

  private boolean runHealthCheckOnPort(KaldbConfigs.ServerConfig serverConfig)
      throws JsonProcessingException {
    String response = getResponse(serverConfig.getServerPort());
    HashMap<String, Object> map = om.readValue(response, HashMap.class);

    LOG.info(String.format("Response from healthcheck - '%s'", response));
    return (boolean) map.get("healthy");
  }

  @Test
  public void testAllComponentsStartSuccessfullyFromConfig() throws JsonProcessingException {
    assertThat(runHealthCheckOnPort(kaldbConfig.getIndexerConfig().getServerConfig()))
        .isEqualTo(true);
    assertThat(runHealthCheckOnPort(kaldbConfig.getQueryConfig().getServerConfig()))
        .isEqualTo(true);
    assertThat(runHealthCheckOnPort(kaldbConfig.getCacheConfig().getServerConfig()))
        .isEqualTo(true);
    assertThat(runHealthCheckOnPort(kaldbConfig.getRecoveryConfig().getServerConfig()))
        .isEqualTo(true);
    assertThat(runHealthCheckOnPort(kaldbConfig.getManagerConfig().getServerConfig()))
        .isEqualTo(true);
  }
}
