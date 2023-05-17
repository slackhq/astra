package com.slack.kaldb.server;

import static com.slack.kaldb.server.ZipkinServiceTest.generateLogWireMessagesForOneTrace;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.logstore.LogWireMessage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ZipkinServiceSpanConversionTest {

  @Test
  public void testLogWireMessageToZipkinSpanConversion() throws InvalidProtocolBufferException {
    Instant time = Instant.now();
    List<LogWireMessage> messages = generateLogWireMessagesForOneTrace(time, 2, "1");

    // follows output format from https://zipkin.io/zipkin-api/#/default/get_trace__traceId_
    String output =
        String.format(
            """
                        [
                            {
                                "traceId": "1",
                                "parentId": "",
                                "id": "1",
                                "kind": "SPAN_KIND_UNSPECIFIED",
                                "name": "Trace1",
                                "timestamp": "%d",
                                "duration": "1",
                                "remoteEndpoint": {
                                    "serviceName": "service1",
                                    "ipv4": "",
                                    "ipv6": "",
                                    "port": 0
                                },
                                "annotations": [],
                                "tags": {},
                                "debug": false,
                                "shared": false
                            },
                            {
                                "traceId": "1",
                                "parentId": "1",
                                "id": "2",
                                "kind": "SPAN_KIND_UNSPECIFIED",
                                "name": "Trace2",
                                "timestamp": "%d",
                                "duration": "2",
                                "remoteEndpoint": {
                                    "serviceName": "service1",
                                    "ipv4": "",
                                    "ipv6": "",
                                    "port": 0
                                },
                                "annotations": [],
                                "tags": {},
                                "debug": false,
                                "shared": false
                            }
                        ]
                        """,
            ZipkinService.convertToMicroSeconds(time.plusSeconds(1)),
            ZipkinService.convertToMicroSeconds(time.plusSeconds(2)));
    assertThat(ZipkinService.convertLogWireMessageToZipkinSpan(messages)).isEqualTo(output);

    assertThat(ZipkinService.convertLogWireMessageToZipkinSpan(new ArrayList<>())).isEqualTo("[]");
  }
}
