package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.server.ValidateKaldbConfig.INDEXER_DATA_TRANSFORMER_MAP;

import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.DocumentBuilder;
import com.slack.kaldb.logstore.LogDocumentBuilderImpl;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.lucene.document.Document;
import org.junit.Test;

public class IndexSerializationTest {

  DocumentBuilder<LogMessage> documentBuilder = LogDocumentBuilderImpl.build(false);

  // Test to see how api-log gets ingested via the pre-processor and then the indexer like it does
  // IRL
  @Test
  public void testIndexingAPILog() throws Exception {

    // Step 1 - Convert murron message to a span
    final String apiLog =
        "{\"ip_address\":\"3.86.63.133\",\"trace_id\":\"123\",\"parent_id\":\"4567\",\"http_method\":\"POST\",\"method\":true,\"enterprise\":\"E012Y1ZD5PU\",\"team\":\"T012YTS8XKM\",\"user\":\"U012YTS942X\",\"status\":\"ok\",\"http_params\":\"flannel_host=flannelbe-dev-iad-iaz2&include_permissions=falseth\",\"ua\":\"Slack-Flannel-Web\\/vef2bd:4046\",\"unique_id\":\"YB2RcPgcUv7PCuIbo8posQAAoDg\",\"request_queue_time\":2262,\"microtime_elapsed\":14168,\"mysql_query_count\":0,\"mysql_query_time\":0,\"mysql_conns_count\":0,\"mysql_conns_time\":0,\"mysql_rows_count\":0,\"mysql_rows_affected\":0,\"mc_queries_count\":11,\"mc_queries_time\":6782,\"frl_time\":0,\"init_time\":1283,\"api_dispatch_time\":0,\"api_output_time\":0,\"api_output_size\":0,\"api_strict\":false,\"ekm_decrypt_reqs_time\":0,\"ekm_decrypt_reqs_count\":0,\"ekm_encrypt_reqs_time\":0,\"ekm_encrypt_reqs_count\":0,\"grpc_req_count\":0,\"grpc_req_time\":0,\"agenda_req_count\":0,\"agenda_req_time\":0,\"trace\":\"#route_main() -> lib_controller.php:69#Controller::handlePost() -> Controller.hack:58#CallbackApiController::handleRequest() -> api.php:45#local_callbacks_api_main_inner() -> api.php:250#api_dispatch() -> lib_api.php:179#api_callbacks_flannel_verifyToken() -> api__callbacks_flannel.php:1714#api_output_fb_thrift() -> lib_api_output.php:390#_api_output_log_call()\",\"client_connection_state\":\"unset\",\"ms_requests_count\":0,\"ms_requests_time\":0,\"token_type\":\"cookie\",\"limited_access_requester_workspace\":\"\",\"limited_access_allowed_workspaces\":\"\",\"repo_auth\":true,\"cf_id\":\"6999afc7b6:haproxy-edge-dev-iad-igu2\",\"external_user\":\"W012XXXFC\",\"timestamp\":\"2021-02-05 10:41:52.340\",\"git_sha\":\"unknown\",\"hhvm_version\":\"4.39.0\",\"slath\":\"callbacks.flannel.verifyToken\",\"php_type\":\"api\",\"webapp_cluster_pbucket\":0,\"webapp_cluster_name\":\"callbacks\",\"webapp_cluster_nest\":\"normal\",\"dev_env\":\"dev-main\",\"pay_product_level\":\"enterprise\",\"type\":\"api_log\",\"level\":\"info\"}";
    Trace.Span span = murronToPreProccessor(apiLog);

    // Step 2 - Take span and write it to downstream topic
    ConsumerRecord<String, byte[]> consumerRecord = spanToDownstreamTopic(span);

    // Step 3 - Consume from kafka, read the byte array, convert it back to span and then to
    // LogMessage
    LogMessage message =
        INDEXER_DATA_TRANSFORMER_MAP.get("trace_span").toLogMessage(consumerRecord).get(0);

    // Step 4 - Take log message and convert it to Lucene Document so that we can index it
    Document document = documentBuilder.fromMessage(message);
    int x = 10;
  }

  private ConsumerRecord<String, byte[]> spanToDownstreamTopic(Trace.Span span) {
    return new ConsumerRecord<>(
        "testTopic",
        1,
        10,
        0L,
        TimestampType.CREATE_TIME,
        0L,
        10,
        1500,
        "testKey",
        span.toByteArray());
  }

  private Trace.Span murronToPreProccessor(String message) {
    String indexName = "api_log";
    String host = "www-host";
    long timestamp = 1612550512340953000L;
    Murron.MurronMessage testMurronMsg =
        Murron.MurronMessage.newBuilder()
            .setMessage(ByteString.copyFrom(message.getBytes(StandardCharsets.UTF_8)))
            .setType(indexName)
            .setHost(host)
            .setTimestamp(timestamp)
            .build();

    ValueMapper<byte[], Iterable<Trace.Span>> valueMapper =
        PreprocessorValueMapper.byteArrayToTraceSpans("api_log");
    byte[] inputBytes =
        KaldbSerdes.MurronMurronMessage().serializer().serialize(indexName, testMurronMsg);

    Iterable<Trace.Span> spanIterable = valueMapper.apply(inputBytes);
    Iterator<Trace.Span> spanIterator = spanIterable.iterator();
    return spanIterator.next();
  }
}
