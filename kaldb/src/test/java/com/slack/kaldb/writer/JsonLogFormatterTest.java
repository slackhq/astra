package com.slack.kaldb.writer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class JsonLogFormatterTest {

  @Test
  public void testJsonByteArrayToTraceSpan() throws IOException {
    String json = "{\"service_name\": \"my-service\", \"@timestamp\": \"2007-12-03T10:15:30.00Z\"}";
    Trace.Span span = JsonLogFormatter.fromJsonLog(json.getBytes(StandardCharsets.UTF_8));
    assertThat(span.getName()).isEqualTo("my-service");
    assertThat(span.getTimestamp()).isEqualTo(1196676930000L); // milliseconds

    json =
        "{\"service_name\": \"my-service\", \"@timestamp\": \"2007-12-03T10:15:30.00Z\", \"field1\" : \"value1\"}";
    span = JsonLogFormatter.fromJsonLog(json.getBytes(StandardCharsets.UTF_8));
    assertThat(span.getTagsList().size()).isEqualTo(3);
    Set<Trace.KeyValue> tags =
        span.getTagsList()
            .stream()
            .filter(kv -> kv.getKey().equals("field1"))
            .collect(Collectors.toSet());
    assertThat(tags.size()).isEqualTo(1);
    assertThat(tags.iterator().next().getVStr()).isEqualTo("value1");
    assertThat(span.getTimestamp()).isEqualTo(1196676930000L); // milliseconds
  }

  @Test
  public void testMissingJsonKeys() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> JsonLogFormatter.fromJsonLog("{}}".getBytes(StandardCharsets.UTF_8)));
    assertThatIOException()
        .isThrownBy(() -> JsonLogFormatter.fromJsonLog("{".getBytes(StandardCharsets.UTF_8)));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                JsonLogFormatter.fromJsonLog(
                    "{\"service_name\": \"my-service\"}".getBytes(StandardCharsets.UTF_8)));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                JsonLogFormatter.fromJsonLog(
                    "{\"@timestamp\": \"2007-12-03T10:15:30.00Z\"}"
                        .getBytes(StandardCharsets.UTF_8)));
  }
}
