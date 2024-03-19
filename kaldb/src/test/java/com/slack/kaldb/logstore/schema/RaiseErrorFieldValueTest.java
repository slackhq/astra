package com.slack.kaldb.logstore.schema;

import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_AND_DUPLICATE_FIELD_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_FIELD_VALUE_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.DROP_FIELDS_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.RAISE_ERROR;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.build;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.kaldb.logstore.FieldDefMismatchException;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.testlib.MetricsUtil;
import com.slack.kaldb.testlib.SpanUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import org.apache.lucene.document.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RaiseErrorFieldValueTest {
  private SimpleMeterRegistry meterRegistry;

  @BeforeEach
  public void setup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
  }

  @Test
  public void testRaiseErrorOnConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(RAISE_ERROR, true, meterRegistry);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
    String conflictingFieldName = "conflictingField";

    Trace.KeyValue hostField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setVStr("host1-dc2.abc.com")
            .build();

    Trace.KeyValue hostFieldAsInt =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setVType(Trace.ValueType.INT32)
            .setVInt32(123)
            .build();

    Trace.KeyValue tagField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.TAG.fieldName)
            .setVStr("foo-bar")
            .build();

    Trace.KeyValue conflictingTagStr =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setVType(Trace.ValueType.STRING)
            .setVStr("1")
            .build();

    Trace.KeyValue conflictingTagInt =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setVType(Trace.ValueType.INT32)
            .setVInt32(1)
            .build();

    Trace.KeyValue newFieldTextTag =
        Trace.KeyValue.newBuilder()
            .setKey("newFieldText")
            .setVType(Trace.ValueType.STRING)
            .setVStr("newFieldValue")
            .build();

    Trace.Span msg1 =
        SpanUtil.makeSpan(
            1, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagInt));

    Document msg1Doc = docBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);
    assertThat(
            msg1Doc.getFields().stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(docBuilder.getSchema().size()).isEqualTo(23);
    assertThat(docBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.INTEGER);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    Trace.Span msg2 =
        SpanUtil.makeSpan(
            2,
            "Test message",
            Instant.now(),
            List.of(hostField, tagField, newFieldTextTag, conflictingTagStr));

    assertThatThrownBy(() -> docBuilder.fromMessage(msg2))
        .isInstanceOf(FieldDefMismatchException.class);
    // NOTE: When a document indexing fails, we still register the types of the fields in this doc.
    // So, the fieldMap may contain an additional item than before.
    assertThat(docBuilder.getSchema().size()).isGreaterThanOrEqualTo(18);
    assertThat(docBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.INTEGER);

    Trace.Span msg3 =
        SpanUtil.makeSpan(
            2, "Test message", Instant.now(), List.of(hostFieldAsInt, tagField, newFieldTextTag));
    assertThatThrownBy(() -> docBuilder.fromMessage(msg3))
        .isInstanceOf(FieldDefMismatchException.class);
    // NOTE: When a document indexing fails, we still register the types of the fields in this doc.
    // So, the fieldMap may contain an additional item than before.
    assertThat(docBuilder.getSchema().size()).isGreaterThanOrEqualTo(18);
    assertThat(docBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.INTEGER);
    assertThat(docBuilder.getSchema().get(LogMessage.ReservedField.HOSTNAME.fieldName).fieldType)
        .isEqualTo(FieldType.STRING);

    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
    assertThat(
            msg1Doc.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isEqualTo(1);
  }

  @Test
  public void testRaiseErrorOnConflictingReservedField() {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(RAISE_ERROR, true, meterRegistry);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
    final String hostNameField = LogMessage.ReservedField.HOSTNAME.fieldName;
    assertThat(docBuilder.getSchema().keySet()).contains(hostNameField);
    assertThat(docBuilder.getSchema().get(hostNameField).fieldType).isEqualTo(FieldType.STRING);

    Trace.KeyValue hostField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setVType(Trace.ValueType.INT32)
            .setVInt32(123)
            .build();

    Trace.KeyValue tagField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.TAG.fieldName)
            .setVStr("foo-bar")
            .build();

    assertThatThrownBy(
            () ->
                docBuilder.fromMessage(
                    SpanUtil.makeSpan(
                        1, "Test message", Instant.now(), List.of(hostField, tagField))))
        .isInstanceOf(FieldDefMismatchException.class);

    assertThat(docBuilder.getSchema().size()).isEqualTo(19);
    assertThat(docBuilder.getSchema().keySet()).contains(hostNameField);
    assertThat(docBuilder.getSchema().get(hostNameField).fieldType).isEqualTo(FieldType.STRING);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
  }

  @Test
  public void testRaiseErrorOnConflictingReservedFieldWithoutFullTextSearch() {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(RAISE_ERROR, false, meterRegistry);
    assertThat(docBuilder.getSchema().size()).isEqualTo(16);
    assertThat(docBuilder.getSchema().keySet())
        .doesNotContain(LogMessage.SystemField.ALL.fieldName);
    final String hostNameField = LogMessage.ReservedField.HOSTNAME.fieldName;
    assertThat(docBuilder.getSchema().keySet()).contains(hostNameField);
    assertThat(docBuilder.getSchema().get(hostNameField).fieldType).isEqualTo(FieldType.STRING);

    Trace.KeyValue hostField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setVType(Trace.ValueType.INT32)
            .setVInt32(123)
            .build();

    Trace.KeyValue tagField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.TAG.fieldName)
            .setVType(Trace.ValueType.STRING)
            .setVStr("foo-bar")
            .build();

    assertThatThrownBy(
            () ->
                docBuilder.fromMessage(
                    SpanUtil.makeSpan(
                        1, "Test message", Instant.now(), List.of(hostField, tagField))))
        .isInstanceOf(FieldDefMismatchException.class);

    assertThat(docBuilder.getSchema().size()).isEqualTo(18);
    assertThat(docBuilder.getSchema().keySet()).contains(hostNameField);
    assertThat(docBuilder.getSchema().get(hostNameField).fieldType).isEqualTo(FieldType.STRING);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
    assertThat(docBuilder.getSchema().keySet())
        .doesNotContain(LogMessage.SystemField.ALL.fieldName);
  }
}
