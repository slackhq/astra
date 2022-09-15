package com.slack.kaldb.zipkinApi;

import static com.slack.kaldb.zipkinApi.ZipkinServiceTest.generateLogWireMessagesForOneTrace;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.logstore.LogWireMessage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class ZipkinServiceSpanConversionTest {

  @Before
  public void setUp() throws Exception {
    ZipkinService.MAX_SPANS = 20_000;
  }

  @Test
  public void testLogWireMessageToZipkinSpanConversion() throws InvalidProtocolBufferException {
    Instant time = Instant.now();
    List<LogWireMessage> messages = generateLogWireMessagesForOneTrace(time, 2, "1");

    // follows output format from https://zipkin.io/zipkin-api/#/default/get_trace__traceId_
    String output =
        String.format(
            "[{\n"
                + "  \"traceId\": \"1\",\n"
                + "  \"parentId\": \"\",\n"
                + "  \"id\": \"1\",\n"
                + "  \"name\": \"Trace1\",\n"
                + "  \"timestamp\": \"%d\",\n"
                + "  \"duration\": \"1\",\n"
                + "  \"tags\": {\n"
                + "  },\n"
                + "  \"remoteEndpoint\": {\n"
                + "    \"serviceName\": \"service1\",\n"
                + "    \"ipv4\": \"\",\n"
                + "    \"ipv6\": \"\",\n"
                + "    \"port\": 0\n"
                + "  }\n"
                + "},{\n"
                + "  \"traceId\": \"1\",\n"
                + "  \"parentId\": \"1\",\n"
                + "  \"id\": \"2\",\n"
                + "  \"name\": \"Trace2\",\n"
                + "  \"timestamp\": \"%d\",\n"
                + "  \"duration\": \"2\",\n"
                + "  \"tags\": {\n"
                + "  },\n"
                + "  \"remoteEndpoint\": {\n"
                + "    \"serviceName\": \"service1\",\n"
                + "    \"ipv4\": \"\",\n"
                + "    \"ipv6\": \"\",\n"
                + "    \"port\": 0\n"
                + "  }\n"
                + "}]",
            ZipkinService.convertToMicroSeconds(time.plusSeconds(1)),
            ZipkinService.convertToMicroSeconds(time.plusSeconds(2)));
    assertThat(ZipkinService.convertLogWireMessageToZipkinSpan(messages)).isEqualTo(output);

    assertThat(ZipkinService.convertLogWireMessageToZipkinSpan(new ArrayList<>())).isEqualTo("[]");
  }
}
