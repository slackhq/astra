package com.slack.kaldb.writer;

import static com.slack.kaldb.writer.ApiLogFormatter.API_LOG_DURATION_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Test;

public class ApiLogFormatterTest {
  public Object getTagValue(List<Trace.KeyValue> tags, String key) {
    for (Trace.KeyValue tag : tags) {
      if (tag.getKey().equals(key)) {
        Trace.ValueType valueType = tag.getVType();
        if (valueType.equals(Trace.ValueType.STRING)) {
          return tag.getVStr();
        }
        if (valueType.equals(Trace.ValueType.INT64)) {
          return tag.getVInt64();
        }
        if (valueType.equals(Trace.ValueType.FLOAT64)) {
          return tag.getVFloat64();
        }
        if (valueType.equals(Trace.ValueType.BOOL)) {
          return tag.getVBool();
        }
        if (valueType.equals(Trace.ValueType.BINARY_VALUE)) {
          return tag.getVBinary();
        }
      }
    }
    return null;
  }

  @Test
  public void testApiSpanConversion() throws JsonProcessingException {
    String message =
        "{\"ip_address\":\"3.86.63.133\",\"http_method\":\"POST\",\"method\":true,\"enterprise\":\"E012Y1ZD5PU\",\"team\":\"T012YTS8XKM\",\"user\":\"U012YTS942X\",\"status\":\"ok\",\"http_params\":\"flannel_host=flannelbe-dev-iad-iaz2&include_permissions=falseth\",\"ua\":\"Slack-Flannel-Web\\/vef2bd:4046\",\"unique_id\":\"YB2RcPgcUv7PCuIbo8posQAAoDg\",\"request_queue_time\":2262,\"microtime_elapsed\":1418,\"mysql_query_count\":0,\"mysql_query_time\":0,\"mysql_conns_count\":0,\"mysql_conns_time\":0,\"mysql_rows_count\":0,\"mysql_rows_affected\":0,\"mc_queries_count\":11,\"mc_queries_time\":6782,\"frl_time\":0,\"init_time\":1283,\"api_dispatch_time\":0,\"api_output_time\":0,\"api_output_size\":0,\"api_strict\":false,\"ekm_decrypt_reqs_time\":0,\"ekm_decrypt_reqs_count\":0,\"ekm_encrypt_reqs_time\":0,\"ekm_encrypt_reqs_count\":0,\"grpc_req_count\":0,\"grpc_req_time\":0,\"agenda_req_count\":0,\"agenda_req_time\":0,\"trace\":\"#route_main() -> lib_controller.php:69#Controller::handlePost() -> Controller.hack:58#CallbackApiController::handleRequest() -> api.php:45#local_callbacks_api_main_inner() -> api.php:250#api_dispatch() -> lib_api.php:179#api_callbacks_flannel_verifyToken() -> api__callbacks_flannel.php:1714#api_output_fb_thrift() -> lib_api_output.php:390#_api_output_log_call()\",\"client_connection_state\":\"unset\",\"ms_requests_count\":0,\"ms_requests_time\":0,\"token_type\":\"cookie\",\"limited_access_requester_workspace\":\"\",\"limited_access_allowed_workspaces\":\"\",\"repo_auth\":true,\"cf_id\":\"6999afc7b6:haproxy-edge-dev-iad-igu2\",\"external_user\":\"W012XXXFC\",\"timestamp\":\"2021-02-05 10:41:52.340\",\"git_sha\":\"unknown\",\"hhvm_version\":\"4.39.0\",\"slath\":\"callbacks.flannel.verifyToken\",\"php_type\":\"api\",\"webapp_cluster_pbucket\":0,\"webapp_cluster_name\":\"callbacks\",\"webapp_cluster_nest\":\"normal\",\"dev_env\":\"dev-main\",\"pay_product_level\":\"enterprise\",\"level\":\"info\"}";
    String indexName = "hhvm-api_log";
    String host = "slack-www-hhvm-dev-dev-callbacks-iad-j8zj";
    long timestamp = 1612550512340953000L;

    // Make a test message
    Murron.MurronMessage.Builder testMurronMsgBuilder = Murron.MurronMessage.newBuilder();
    testMurronMsgBuilder
        .setMessage(ByteString.copyFrom(message.getBytes(StandardCharsets.UTF_8)))
        .setType(indexName)
        .setHost(host)
        .setTimestamp(timestamp);
    Murron.MurronMessage testMurronMsg = testMurronMsgBuilder.build();

    Trace.Span apiSpan = ApiLogFormatter.toSpan(testMurronMsg);
    assertThat(apiSpan.getDurationMicros()).isEqualTo(1418L);
    assertThat(apiSpan.getName()).isEmpty();
    assertThat(apiSpan.getId().toString().contains(host)).isTrue();
    assertThat(apiSpan.getTraceId().isEmpty()).isTrue();
    assertThat(apiSpan.getParentId().isEmpty()).isTrue();
    assertThat(apiSpan.getStartTimestampMicros()).isEqualTo(timestamp / 1000);
    List<Trace.KeyValue> tags = apiSpan.getTagsList();
    assertThat(getTagValue(tags, "http_method")).isEqualTo("POST");
    assertThat(getTagValue(tags, "mysql_conns_count")).isEqualTo(0L);
    assertThat((boolean) getTagValue(tags, "method")).isTrue();
    assertThat(getTagValue(tags, API_LOG_DURATION_FIELD)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.PARENT_ID.fieldName)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.TRACE_ID.fieldName)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.HOSTNAME.fieldName)).isEqualTo(host);
    assertThat(getTagValue(tags, LogMessage.ReservedField.SERVICE_NAME.fieldName))
        .isEqualTo("hhvm_api_log");
  }

  @Test
  public void testApiSpanWithTraceContextConversion() throws JsonProcessingException {
    String message =
        "{\"ip_address\":\"3.86.63.133\",\"trace_id\":\"123\",\"parent_id\":\"4567\",\"http_method\":\"POST\",\"method\":true,\"enterprise\":\"E012Y1ZD5PU\",\"team\":\"T012YTS8XKM\",\"user\":\"U012YTS942X\",\"status\":\"ok\",\"http_params\":\"flannel_host=flannelbe-dev-iad-iaz2&include_permissions=falseth\",\"ua\":\"Slack-Flannel-Web\\/vef2bd:4046\",\"unique_id\":\"YB2RcPgcUv7PCuIbo8posQAAoDg\",\"request_queue_time\":2262,\"microtime_elapsed\":14168,\"mysql_query_count\":0,\"mysql_query_time\":0,\"mysql_conns_count\":0,\"mysql_conns_time\":0,\"mysql_rows_count\":0,\"mysql_rows_affected\":0,\"mc_queries_count\":11,\"mc_queries_time\":6782,\"frl_time\":0,\"init_time\":1283,\"api_dispatch_time\":0,\"api_output_time\":0,\"api_output_size\":0,\"api_strict\":false,\"ekm_decrypt_reqs_time\":0,\"ekm_decrypt_reqs_count\":0,\"ekm_encrypt_reqs_time\":0,\"ekm_encrypt_reqs_count\":0,\"grpc_req_count\":0,\"grpc_req_time\":0,\"agenda_req_count\":0,\"agenda_req_time\":0,\"trace\":\"#route_main() -> lib_controller.php:69#Controller::handlePost() -> Controller.hack:58#CallbackApiController::handleRequest() -> api.php:45#local_callbacks_api_main_inner() -> api.php:250#api_dispatch() -> lib_api.php:179#api_callbacks_flannel_verifyToken() -> api__callbacks_flannel.php:1714#api_output_fb_thrift() -> lib_api_output.php:390#_api_output_log_call()\",\"client_connection_state\":\"unset\",\"ms_requests_count\":0,\"ms_requests_time\":0,\"token_type\":\"cookie\",\"limited_access_requester_workspace\":\"\",\"limited_access_allowed_workspaces\":\"\",\"repo_auth\":true,\"cf_id\":\"6999afc7b6:haproxy-edge-dev-iad-igu2\",\"external_user\":\"W012XXXFC\",\"timestamp\":\"2021-02-05 10:41:52.340\",\"git_sha\":\"unknown\",\"hhvm_version\":\"4.39.0\",\"slath\":\"callbacks.flannel.verifyToken\",\"php_type\":\"api\",\"webapp_cluster_pbucket\":0,\"webapp_cluster_name\":\"callbacks\",\"webapp_cluster_nest\":\"normal\",\"dev_env\":\"dev-main\",\"pay_product_level\":\"enterprise\",\"type\":\"api_log\",\"level\":\"info\"}";
    String indexName = "hhvm-api_log";
    String host = "slack-www-hhvm-dev-dev-callbacks-iad-j8zj";
    long timestamp = 1612550512340953000L;

    // Make a test message
    Murron.MurronMessage.Builder testMurronMsgBuilder = Murron.MurronMessage.newBuilder();
    testMurronMsgBuilder
        .setMessage(ByteString.copyFrom(message.getBytes(StandardCharsets.UTF_8)))
        .setType(indexName)
        .setHost(host)
        .setTimestamp(timestamp);
    Murron.MurronMessage testMurronMsg = testMurronMsgBuilder.build();

    Trace.Span apiSpan = ApiLogFormatter.toSpan(testMurronMsg);
    assertThat(apiSpan.getName()).isEqualTo("api_log");
    assertThat(apiSpan.getDurationMicros()).isEqualTo(14168L);
    assertThat(apiSpan.getId().toString().contains(host)).isTrue();
    assertThat(apiSpan.getTraceId().toStringUtf8()).isEqualTo("123");
    assertThat(apiSpan.getParentId().toStringUtf8()).isEqualTo("4567");
    assertThat(apiSpan.getStartTimestampMicros()).isEqualTo(timestamp / 1000);
    List<Trace.KeyValue> tags = apiSpan.getTagsList();
    assertThat(getTagValue(tags, "http_method")).isEqualTo("POST");
    assertThat(getTagValue(tags, "mysql_conns_count")).isEqualTo(0L);
    assertThat((boolean) getTagValue(tags, "method")).isTrue();
    assertThat(getTagValue(tags, API_LOG_DURATION_FIELD)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.PARENT_ID.fieldName)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.TRACE_ID.fieldName)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.HOSTNAME.fieldName)).isEqualTo(host);
    assertThat(getTagValue(tags, LogMessage.ReservedField.SERVICE_NAME.fieldName))
        .isEqualTo("hhvm_api_log");
  }

  @Test
  public void testNullMurronMessage() throws JsonProcessingException {
    ApiLogFormatter.toSpan(null);
  }

  @Test(expected = MismatchedInputException.class)
  public void testEmptyMurronMessage() throws JsonProcessingException {
    ApiLogFormatter.toSpan(Murron.MurronMessage.newBuilder().build());
  }
}
