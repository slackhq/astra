package com.slack.kaldb.zipkinApi;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ZipkinServiceTest {

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ZipkinService zipkinService;

  private SimpleMeterRegistry metricsRegistry;
  private ChunkManagerUtil<LogMessage> chunkManagerUtil;

  private static final String TEST_KAFKA_PARTITION_ID = "10";

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        ChunkManagerUtil.makeChunkManagerUtil(
            S3_MOCK_RULE,
            metricsRegistry,
            10 * 1024 * 1024 * 1024L,
            1000000L,
            KaldbConfigUtil.makeIndexerConfig());
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
    //    LogMessage msg100 = MessageUtil.makeMessage(100);
    //    MessageUtil.addFieldToMessage(msg100, LogMessage.ReservedField.TRACE_ID.fieldName,
    // "1111");
    //    chunkManagerUtil.chunkManager.addMessage(
    //        msg100, msg100.toString().length(), TEST_KAFKA_PARTITION_ID, 1);
    KaldbLocalQueryService<LogMessage> searcher =
        new KaldbLocalQueryService<>(chunkManagerUtil.chunkManager, Duration.ofSeconds(3));
    zipkinService = new ZipkinService(searcher);
  }

  @After
  public void tearDown() throws TimeoutException, IOException {
    chunkManagerUtil.close();
    metricsRegistry.close();
  }

  @Test
  public void testEmptySearch() throws IOException {
    HttpResponse response = zipkinService.getTraceByTraceId("test");
    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    assertThat(body.equals("[]"));
    assertThat(aggregatedRes.status().code()).isEqualTo(200);
  }

  @Test
  public void testMultipleResultsSearch() throws Exception {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages =
        MessageUtil.makeTraceMessagesWithTimeDifference(
            1, 100, 1000, startTime, "testTraceId", "testParentId", "testName", "testServiceName");
    int offset = 1;
    for (LogMessage m : messages) {
      m.addProperty(LogMessage.ReservedField.TRACE_ID.fieldName, "1111");
      chunkManager.addMessage(m, m.toString().length(), "10", offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();
    HttpResponse response = zipkinService.getTraceByTraceId("1111");
    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    assertThat(body.length() == 10);
    assertThat(aggregatedRes.status().code()).isEqualTo(200);
  }
}
