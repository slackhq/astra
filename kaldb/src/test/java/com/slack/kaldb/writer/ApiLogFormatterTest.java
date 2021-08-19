package com.slack.kaldb.writer;

import static com.slack.kaldb.writer.ApiLogFormatter.API_LOG_DURATION_FIELD;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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
        "{\"ip_address\":\"127.0.0.1\",\"trace_id\":\"123\",\"parent_id\":\"4567\",\"http_method\":\"POST\",\"method\":true,\"enterprise\":\"E1234ABCD56\",\"team\":\"T98765XYZ12\",\"user\":\"U000111222A\",\"status\":\"ok\",\"http_params\":\"param1=value1&param2=value2&param3=false\",\"ua\":\"Hello-World-Web\\/vef2bd:1234\",\"unique_id\":\"YBBccDDuu17CxYza6abcDEFzYzz\",\"request_queue_time\":2262,\"microtime_elapsed\":1418,\"mysql_query_count\":0,\"mysql_query_time\":0,\"mysql_conns_count\":0,\"mysql_conns_time\":0,\"mysql_rows_count\":0,\"mysql_rows_affected\":0,\"my_queries_count\":11,\"my_queries_time\":6782,\"frl_time\":0,\"init_time\":1283,\"api_dispatch_time\":0,\"api_output_time\":0,\"api_output_size\":0,\"api_strict\":false,\"decrypt_reqs_time\":0,\"decrypt_reqs_count\":0,\"encrypt_reqs_time\":0,\"encrypt_reqs_count\":0,\"grpc_req_count\":0,\"grpc_req_time\":0,\"service_req_count\":0,\"service_req_time\":0,\"trace\":\"#route_main() -> lib_controller.php:12#Controller::handlePost() -> Controller.php:58#CallbackApiController::handleRequest() -> api.php:100#local_callbacks_api_main_inner() -> api.php:250#api_dispatch() -> lib_api.php:000#api_callbacks_service_verifyToken() -> api__callbacks_service.php:1500#api_output_fb_thrift() -> lib_api_output.php:390#_api_output_log_call()\",\"client_connection_state\":\"unset\",\"ms_requests_count\":0,\"ms_requests_time\":0,\"token_type\":\"cookie\",\"another_param\":\"\",\"another_value\":\"\",\"auth\":true,\"ab_id\":\"1234abc12d:host-abc-dev-region-1234\",\"external_user\":\"W012XYZAB\",\"timestamp\":\"2021-02-05 10:41:52.340\",\"sha\":\"unknown\",\"php_version\":\"5.11.0\",\"paramX\":\"yet.another.value\",\"php_type\":\"api\",\"bucket_type_something\":0,\"cluster_name\":\"cluster\",\"cluster_param\":\"normal\",\"env\":\"env-value\",\"type\":\"api_log\",\"level\":\"info\"};";
    String indexName = "hhvm-api_log";
    String host = "company-www-php-dev-cluster-abc-x8ab";
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
        "{\"ip_address\":\"127.0.0.1\",\"trace_id\":\"123\",\"parent_id\":\"4567\",\"http_method\":\"POST\",\"method\":true,\"enterprise\":\"E1234ABCD56\",\"team\":\"T98765XYZ12\",\"user\":\"U000111222A\",\"status\":\"ok\",\"http_params\":\"param1=value1&param2=value2&param3=false\",\"ua\":\"Hello-World-Web\\/vef2bd:1234\",\"unique_id\":\"YBBccDDuu17CxYza6abcDEFzYzz\",\"request_queue_time\":2262,\"microtime_elapsed\":1418,\"mysql_query_count\":0,\"mysql_query_time\":0,\"mysql_conns_count\":0,\"mysql_conns_time\":0,\"mysql_rows_count\":0,\"mysql_rows_affected\":0,\"my_queries_count\":11,\"my_queries_time\":6782,\"frl_time\":0,\"init_time\":1283,\"api_dispatch_time\":0,\"api_output_time\":0,\"api_output_size\":0,\"api_strict\":false,\"decrypt_reqs_time\":0,\"decrypt_reqs_count\":0,\"encrypt_reqs_time\":0,\"encrypt_reqs_count\":0,\"grpc_req_count\":0,\"grpc_req_time\":0,\"service_req_count\":0,\"service_req_time\":0,\"trace\":\"#route_main() -> lib_controller.php:12#Controller::handlePost() -> Controller.php:58#CallbackApiController::handleRequest() -> api.php:100#local_callbacks_api_main_inner() -> api.php:250#api_dispatch() -> lib_api.php:000#api_callbacks_service_verifyToken() -> api__callbacks_service.php:1500#api_output_fb_thrift() -> lib_api_output.php:390#_api_output_log_call()\",\"client_connection_state\":\"unset\",\"ms_requests_count\":0,\"ms_requests_time\":0,\"token_type\":\"cookie\",\"another_param\":\"\",\"another_value\":\"\",\"auth\":true,\"ab_id\":\"1234abc12d:host-abc-dev-region-1234\",\"external_user\":\"W012XYZAB\",\"timestamp\":\"2021-02-05 10:41:52.340\",\"sha\":\"unknown\",\"php_version\":\"5.11.0\",\"paramX\":\"yet.another.value\",\"php_type\":\"api\",\"bucket_type_something\":0,\"cluster_name\":\"cluster\",\"cluster_param\":\"normal\",\"env\":\"env-value\",\"type\":\"api_log\",\"level\":\"info\"};";
    String indexName = "hhvm-api_log";
    String host = "company-www-php-dev-cluster-abc-x8ab";
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
    assertThat(apiSpan.getDurationMicros()).isEqualTo(1418L);
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
