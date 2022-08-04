package zipkinApi;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.elasticsearchApi.ElasticsearchApiService;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.zipkinApi.ZipkinService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ZipkinServiceTest {

    @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
    private static final String TEST_KAFKA_PARTITION_ID = "10";
    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private ZipkinService zipkinService;

    private SimpleMeterRegistry metricsRegistry;
    private ChunkManagerUtil<LogMessage> chunkManagerUtil;

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
        KaldbLocalQueryService<LogMessage> searcher =
                new KaldbLocalQueryService<>(chunkManagerUtil.chunkManager);
        zipkinService = new ZipkinService(searcher);
    }

    @After
    public void tearDown() throws TimeoutException, IOException {
        chunkManagerUtil.close();
        metricsRegistry.close();
    }



    @Test
    public void testEmptySearchGrafana7() throws IOException {
//       String postBody =
//                Resources.toString(
//                        Resources.getResource("elasticsearchApi/empty_search_grafana7.ndjson"),
//                        Charset.defaultCharset());
        HttpResponse response = zipkinService.getTraceByTraceId("test");

        // handle response
        AggregatedHttpResponse aggregatedRes = response.aggregate().join();
        String body = aggregatedRes.content(StandardCharsets.UTF_8);
        JsonNode jsonNode = new ObjectMapper().readTree(body);

        assertThat(aggregatedRes.status().code()).isEqualTo(200);
        assertThat(jsonNode.size() == 0);
    }
}
