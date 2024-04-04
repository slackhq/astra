package com.slack.astra.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.testlib.MessageUtil;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class JsonUtilTest {

  @Test
  public void simpleJSONSerDe() throws IOException {
    LogMessage message = MessageUtil.makeMessage(1);
    String serializedMsg = JsonUtil.writeAsString(message.toWireMessage());
    LogWireMessage newMsg = JsonUtil.read(serializedMsg, LogWireMessage.class);
    assertThat(newMsg.getId()).isEqualTo(message.getId());
    assertThat(newMsg.getIndex()).isEqualTo(message.getIndex());
    assertThat(newMsg.getType()).isEqualTo(message.getType());
  }

  // TODO: Add additional tests to test the mapper flags and configs.
}
