package com.slack.kaldb.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.kaldb.config.KaldbConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
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
  private final ObjectMapper om = new ObjectMapper();

  @Before
  public void start() throws IOException {
    KaldbConfig.reset();
    kaldb = new Kaldb(Path.of("../config/config.yaml"));
    LOG.info("Starting kalDb with the resolved configs: {}", KaldbConfig.get().toString());
    kaldb.start();
    kaldb.serviceManager.awaitHealthy();
  }

  @After
  public void shutdown() {
    kaldb.shutdown();
    KaldbConfig.reset();
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

  @Test
  public void testIndexStartupHealthcheck() throws JsonProcessingException {
    String response =
        getResponse(
            String.format(
                "http://localhost:%s/health",
                KaldbConfig.get().getIndexerConfig().getServerConfig().getServerPort()));
    HashMap<String, Object> map = om.readValue(response, HashMap.class);

    LOG.info(String.format("Response from healthcheck - '%s'", response));
    assertThat(map.get("healthy")).isEqualTo(true);
  }

  @Test
  public void testQueryStartupHealthcheck() throws JsonProcessingException {
    String response =
        getResponse(
            String.format(
                "http://localhost:%s/health",
                KaldbConfig.get().getQueryConfig().getServerConfig().getServerPort()));
    HashMap<String, Object> map = om.readValue(response, HashMap.class);

    LOG.info(String.format("Response from healthcheck - '%s'", response));
    assertThat(map.get("healthy")).isEqualTo(true);
  }
}
