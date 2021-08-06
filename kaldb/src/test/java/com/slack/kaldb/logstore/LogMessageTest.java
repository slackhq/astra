package com.slack.kaldb.logstore;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.logstore.LogMessage.SystemField;
import org.junit.Test;

public class LogMessageTest {

  @Test
  public void testSystemField() {
    assertThat(LogMessage.SystemField.values().length).isEqualTo(5);
    assertThat(SystemField.systemFieldNames.size()).isEqualTo(5);
    assertThat(LogMessage.SystemField.isSystemField("_source")).isTrue();

    assertThat(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName).isEqualTo("_timesinceepoch");
    for (LogMessage.SystemField f : LogMessage.SystemField.values()) {
      if (!(f.equals(LogMessage.SystemField.SOURCE)
          || f.equals(LogMessage.SystemField.TIME_SINCE_EPOCH))) {
        assertThat(f.fieldName).isEqualTo(f.name().toLowerCase());
      }
    }
    assertThat(LogMessage.SystemField.isSystemField("test")).isFalse();
  }

  @Test
  public void testReservedField() {
    assertThat(LogMessage.ReservedField.values().length).isEqualTo(12);
    assertThat(LogMessage.ReservedField.reservedFieldNames.size()).isEqualTo(12);
    assertThat(LogMessage.ReservedField.isReservedField("hostname")).isTrue();
    assertThat(LogMessage.ReservedField.TIMESTAMP.fieldName).isEqualTo("@timestamp");
    for (LogMessage.ReservedField f : LogMessage.ReservedField.values()) {
      if (!f.equals(LogMessage.ReservedField.TIMESTAMP)) {
        assertThat(f.name().toLowerCase()).isEqualTo(f.fieldName);
      }
    }
    assertThat(LogMessage.ReservedField.isReservedField("test")).isFalse();
  }
}
