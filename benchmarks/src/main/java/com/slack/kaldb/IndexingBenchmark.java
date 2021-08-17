package com.slack.kaldb;

import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.DocumentBuilder;
import com.slack.kaldb.logstore.LogDocumentBuilderImpl;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.service.murron.Murron;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.openjdk.jmh.annotations.*;

@State(Scope.Thread)
public class IndexingBenchmark {

  private final Duration commitInterval = Duration.ofSeconds(5 * 60);
  private final Duration refreshInterval = Duration.ofSeconds(5 * 60);

  private Path tempDirectory;
  private MeterRegistry registry;
  LuceneIndexStoreImpl logStore;
  private Random random;

  private ConsumerRecord<String, byte[]> kafkaRecord;
  private LogMessage logMessage;
  private Document luceneDocument;

  @Setup(Level.Iteration)
  public void createIndexer() throws Exception {
    random = new Random();
    registry = new SimpleMeterRegistry();
    tempDirectory =
        Files.createDirectories(
            Paths.get("jmh-output", String.valueOf(random.nextInt(Integer.MAX_VALUE))));
    logStore =
        LuceneIndexStoreImpl.makeLogStore(
            tempDirectory.toFile(), commitInterval, refreshInterval, registry);

    String message =
        "{\"ip_address\":\"3.86.63.133\",\"http_method\":\"POST\",\"method\":\"callbacks.flannel.verifyToken\","
            + "\"enterprise\":\"E012Y1ZD5PU\",\"team\":\"T012YTS8XKM\",\"user\":\"U012YTS942X\",\"status\":\"ok\","
            + "\"http_params\":\"flannel_host=flannelbe-dev-iad-iaz2&include_permissions=falseth\","
            + "\"ua\":\"Slack-Flannel-Web\\/vef2bd:4046\",\"unique_id\":\"YB2RcPgcUv7PCuIbo8posQAAoDg\","
            + "\"request_queue_time\":2262,\"microtime_elapsed\":14168,\"mysql_query_count\":0,\"mysql_query_time\":0,"
            + "\"mysql_conns_count\":0,\"mysql_conns_time\":0,\"mysql_rows_count\":0,\"mysql_rows_affected\":0,"
            + "\"mc_queries_count\":11,\"mc_queries_time\":6782,\"frl_time\":0,\"init_time\":1283,"
            + "\"api_dispatch_time\":0,\"api_output_time\":0,\"api_output_size\":0,\"api_strict\":false,"
            + "\"ekm_decrypt_reqs_time\":0,\"ekm_decrypt_reqs_count\":0,\"ekm_encrypt_reqs_time\":0,"
            + "\"ekm_encrypt_reqs_count\":0,\"grpc_req_count\":0,\"grpc_req_time\":0,\"agenda_req_count\":0,"
            + "\"agenda_req_time\":0,\"trace\":\"#route_main() -> lib_controller.php:69#Controller::handlePost() "
            + "-> Controller.hack:58#CallbackApiController::handleRequest() -> api.php:45#local_callbacks_api_main_inner() "
            + "-> api.php:250#api_dispatch() -> lib_api.php:179#api_callbacks_flannel_verifyToken() ->"
            + " api__callbacks_flannel.php:1714#api_output_fb_thrift() -> lib_api_output.php:390#_api_output_log_call()\","
            + "\"client_connection_state\":\"unset\",\"ms_requests_count\":0,\"ms_requests_time\":0,"
            + "\"token_type\":\"cookie\",\"limited_access_requester_workspace\":\"\",\"limited_access_allowed_workspaces\":\"\","
            + "\"repo_auth\":true,\"cf_id\":\"6999afc7b6:haproxy-edge-dev-iad-igu2\",\"external_user\":\"W012XXXFC\","
            + "\"timestamp\":\"2021-02-05 10:41:52.340\",\"git_sha\":\"unknown\",\"hhvm_version\":\"4.39.0\","
            + "\"slath\":\"callbacks.flannel.verifyToken\",\"php_type\":\"api\",\"webapp_cluster_pbucket\":0,"
            + "\"webapp_cluster_name\":\"callbacks\",\"webapp_cluster_nest\":\"normal\","
            + "\"dev_env\":\"dev-main\",\"pay_product_level\":\"enterprise\",\"type\":\"api_log\",\"level\":\"info\"}";

    String indexName = "hhvm-api_log";
    String host = "slack-www-hhvm-dev-dev-callbacks-iad-j8zj";
    long timestamp = 1612550512340953000L;
    Murron.MurronMessage testMurronMsg =
        Murron.MurronMessage.newBuilder()
            .setMessage(ByteString.copyFrom(message.getBytes(StandardCharsets.UTF_8)))
            .setType(indexName)
            .setHost(host)
            .setTimestamp(timestamp)
            .build();

    kafkaRecord =
        new ConsumerRecord<>(
            "testTopic",
            1,
            10,
            0L,
            TimestampType.CREATE_TIME,
            0L,
            0,
            0,
            "testKey",
            testMurronMsg.toByteString().toByteArray());

    logMessage = LogMessageWriterImpl.apiLogTransformer.toLogMessage(kafkaRecord).get(0);

    DocumentBuilder<LogMessage> documentBuilder = LogDocumentBuilderImpl.build(false);

    luceneDocument = documentBuilder.fromMessage(logMessage);
  }

  @TearDown(Level.Iteration)
  public void tearDown() throws IOException {
    logStore.close();
    try (Stream<Path> walk = Files.walk(tempDirectory)) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
    registry.close();
  }

  @Benchmark
  public void measureIndexingAsKafkaSerializedDocument() throws Exception {
    // Mimic LogMessageWriterImpl#insertRecord kinda without the chunk rollover logic
    LogMessage localLogMessage =
        LogMessageWriterImpl.apiLogTransformer.toLogMessage(kafkaRecord).get(0);
    logStore.addMessage(localLogMessage);
  }

  @Benchmark
  public void measureIndexingAsLogMessage() {
    logStore.addMessage(logMessage);
  }

  @Benchmark
  public void measureIndexingAsLuceneDocument() {
    IndexWriter indexWriter = logStore.getIndexWriter();
    try {
      indexWriter.addDocument(luceneDocument);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
