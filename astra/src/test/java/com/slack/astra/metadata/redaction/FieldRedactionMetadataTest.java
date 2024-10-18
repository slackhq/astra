package com.slack.astra.metadata.redaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class FieldRedactionMetadataTest {

  @Test
  public void testFieldRedactionMetadata() {
    final String name = "testRedaction";
    final String fieldName = "testField";
    final long start = Instant.now().toEpochMilli();
    final long end = Instant.now().plusSeconds(10).toEpochMilli();

    FieldRedactionMetadata fieldRedactionMetadata =
        new FieldRedactionMetadata(name, fieldName, start, end);

    assertThat(fieldRedactionMetadata.name).isEqualTo(name);
    assertThat(fieldRedactionMetadata.fieldName).isEqualTo(fieldName);
    assertThat(fieldRedactionMetadata.startTimeEpochMs).isEqualTo(start);
    assertThat(fieldRedactionMetadata.endTimeEpochMs).isEqualTo(end);
  }

  @Test
  public void testValidFieldRedactionMetadata() {
    final String name = "testRedaction";
    final String fieldName = "testField";
    final long start = Instant.now().toEpochMilli();
    final long end = Instant.now().plusSeconds(10).toEpochMilli();

    // invalid redaction name
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new FieldRedactionMetadata("", fieldName, start, end));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new FieldRedactionMetadata(null, fieldName, start, end));
    // invalid field name
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new FieldRedactionMetadata(name, "", start, end));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new FieldRedactionMetadata(name, null, start, end));
    // start time == 0
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new FieldRedactionMetadata(name, fieldName, 0L, end));
    // start time > end time
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new FieldRedactionMetadata(name, fieldName, end, start));
  }

  @Test
  public void testInRedactionTimerange() {
    final String name = "testRedaction";
    final String fieldName = "testField";
    final Instant time = Instant.now();
    final long start = time.toEpochMilli();
    final long end = time.plusSeconds(10).toEpochMilli();

    FieldRedactionMetadata fieldRedactionMetadata =
        new FieldRedactionMetadata(name, fieldName, start, end);

    assertThat(fieldRedactionMetadata.inRedactionTimerange(start)).isTrue();
    assertThat(fieldRedactionMetadata.inRedactionTimerange(end)).isTrue();
    assertThat(fieldRedactionMetadata.inRedactionTimerange(time.plusSeconds(5).toEpochMilli()))
        .isTrue();
    assertThat(fieldRedactionMetadata.inRedactionTimerange(time.minusSeconds(10).toEpochMilli()))
        .isFalse();
    assertThat(fieldRedactionMetadata.inRedactionTimerange(time.plusSeconds(15).toEpochMilli()))
        .isFalse();
  }
}
