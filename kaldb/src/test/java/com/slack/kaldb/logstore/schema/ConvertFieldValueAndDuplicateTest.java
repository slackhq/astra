package com.slack.kaldb.logstore.schema;

import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_AND_DUPLICATE_FIELD_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_ERROR_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_FIELD_VALUE_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.DROP_FIELDS_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_VALUE_AND_DUPLICATE_FIELD;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.build;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.makeNewFieldOfType;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.testlib.MetricsUtil;
import com.slack.kaldb.testlib.SpanUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConvertFieldValueAndDuplicateTest {
  private SimpleMeterRegistry meterRegistry;

  @BeforeEach
  public void setup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
  }

  @Test
  public void testListTypeInDocument() throws IOException {
    // TODO:
  }

  @Test
  public void testListTypeInDocumentWithoutFullTextSearch() throws IOException {
    // TODO:
  }

  @Test
  public void testConvertingAndDuplicatingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        build(CONVERT_VALUE_AND_DUPLICATE_FIELD, true, meterRegistry);
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(17);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(LogMessage.SystemField.ALL.fieldName);
    String conflictingFieldName = "conflictingField";

    Trace.KeyValue hostField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setVStr("host1-dc2.abc.com")
            .setVType(Trace.ValueType.STRING)
            .build();
    Trace.KeyValue tagField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.TAG.fieldName)
            .setVType(Trace.ValueType.STRING)
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

    Trace.Span msg1 =
        SpanUtil.makeSpan(
            1, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagStr));

    Document msg1Doc = convertFieldBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);
    assertThat(
            msg1Doc.getFields().stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(23);
    assertThat(convertFieldBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    Trace.Span msg2 =
        SpanUtil.makeSpan(
            1, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagInt));
    Document msg2Doc = convertFieldBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(31);
    String additionalCreatedFieldName = makeNewFieldOfType(conflictingFieldName, FieldType.INTEGER);
    // Value converted and new field is added.
    assertThat(
            msg2Doc.getFields().stream()
                .filter(
                    f ->
                        f.name().equals(conflictingFieldName)
                            || f.name().equals(additionalCreatedFieldName))
                .count())
        .isEqualTo(4);
    assertThat(msg2Doc.getField(conflictingFieldName).stringValue()).isEqualTo("1");
    // Field value is null since we don't store the int field anymore.
    assertThat(msg2Doc.getField(additionalCreatedFieldName).stringValue()).isNull();
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(24);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(conflictingFieldName, additionalCreatedFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(convertFieldBuilder.getSchema().get(additionalCreatedFieldName).fieldType)
        .isEqualTo(FieldType.INTEGER);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry))
        .isEqualTo(1);
    assertThat(
            msg1Doc.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isEqualTo(1);
    assertThat(
            msg2Doc.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isEqualTo(1);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(LogMessage.SystemField.ALL.fieldName);
  }

  @Test
  public void testConvertingAndDuplicatingConflictingBooleanField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        build(CONVERT_VALUE_AND_DUPLICATE_FIELD, true, meterRegistry);
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(17);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(LogMessage.SystemField.ALL.fieldName);
    String conflictingFieldName = "conflictingField";

    Trace.KeyValue hostField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setVType(Trace.ValueType.STRING)
            .setVStr("host1-dc2.abc.com")
            .build();
    Trace.KeyValue tagField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.TAG.fieldName)
            .setVType(Trace.ValueType.STRING)
            .setVStr("foo-bar")
            .build();

    Trace.KeyValue conflictingTagBool =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setVType(Trace.ValueType.BOOL)
            .setVBool(true)
            .build();

    Trace.KeyValue conflictingTagStr =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setVType(Trace.ValueType.STRING)
            .setVStr("random")
            .build();

    Trace.Span msg1 =
        SpanUtil.makeSpan(
            1, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagBool));

    Document msg1Doc = convertFieldBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);
    assertThat(
            msg1Doc.getFields().stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(23);
    assertThat(convertFieldBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.BOOLEAN);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    Trace.Span msg2 =
        SpanUtil.makeSpan(
            2, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagStr));

    Document msg2Doc = convertFieldBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(31);
    String additionalCreatedFieldName = makeNewFieldOfType(conflictingFieldName, FieldType.STRING);
    // Value converted and new field is added.
    assertThat(
            msg2Doc.getFields().stream()
                .filter(
                    f ->
                        f.name().equals(conflictingFieldName)
                            || f.name().equals(additionalCreatedFieldName))
                .count())
        .isEqualTo(4);
    assertThat(msg2Doc.getField(conflictingFieldName).binaryValue()).isEqualTo(new BytesRef("F"));
    // Field value is null since we don't store the int field anymore.
    assertThat(msg2Doc.getField(additionalCreatedFieldName).stringValue()).isEqualTo("random");
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(24);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(conflictingFieldName, additionalCreatedFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.BOOLEAN);
    assertThat(convertFieldBuilder.getSchema().get(additionalCreatedFieldName).fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_ERROR_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry))
        .isEqualTo(1);
    assertThat(
            msg1Doc.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isEqualTo(1);
    assertThat(
            msg2Doc.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isEqualTo(1);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(LogMessage.SystemField.ALL.fieldName);

    // We now want to test conversion of boolean to a text field
    String additionalCreatedBoolFieldName =
        makeNewFieldOfType(additionalCreatedFieldName, FieldType.BOOLEAN);

    Trace.KeyValue additionalCreatedBoolFieldNameTag =
        Trace.KeyValue.newBuilder()
            .setKey(additionalCreatedFieldName)
            .setVType(Trace.ValueType.BOOL)
            .setVBool(true)
            .build();
    Trace.Span msg3 =
        SpanUtil.makeSpan(
            3,
            "Test message",
            Instant.now(),
            List.of(hostField, tagField, additionalCreatedBoolFieldNameTag));
    Document msg3Doc = convertFieldBuilder.fromMessage(msg3);
    assertThat(msg3Doc.getFields().size()).isEqualTo(31);
    assertThat(
            msg3Doc.getFields().stream()
                .filter(
                    f ->
                        f.name().equals(additionalCreatedBoolFieldName)
                            || f.name().equals(additionalCreatedFieldName))
                .count())
        .isEqualTo(4);
    assertThat(msg3Doc.getField(additionalCreatedFieldName).stringValue()).isEqualTo("true");
    assertThat(msg3Doc.getField(additionalCreatedBoolFieldName).binaryValue())
        .isEqualTo(new BytesRef("T"));
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(25);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(conflictingFieldName, additionalCreatedFieldName, additionalCreatedBoolFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.BOOLEAN);
    assertThat(convertFieldBuilder.getSchema().get(additionalCreatedFieldName).fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(convertFieldBuilder.getSchema().get(additionalCreatedBoolFieldName).fieldType)
        .isEqualTo(FieldType.BOOLEAN);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    // was not able to parse "random" into a boolean field
    assertThat(MetricsUtil.getCount(CONVERT_ERROR_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry))
        .isEqualTo(2);
  }

  @Test
  public void testValueTypeConversionWorksInDocument() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        build(CONVERT_VALUE_AND_DUPLICATE_FIELD, true, meterRegistry);
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(17);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(LogMessage.SystemField.ALL.fieldName);
    String conflictingFieldName = "conflictingField";

    Trace.KeyValue hostField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setVType(Trace.ValueType.STRING)
            .setVStr("host1-dc2.abc.com")
            .build();
    Trace.KeyValue tagField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.TAG.fieldName)
            .setVType(Trace.ValueType.STRING)
            .setVStr("foo-bar")
            .build();

    Trace.KeyValue conflictingTagStr =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setVType(Trace.ValueType.STRING)
            .setVStr("1")
            .build();

    Trace.Span msg1 =
        SpanUtil.makeSpan(
            1, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagStr));

    Document msg1Doc = convertFieldBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);
    assertThat(
            msg1Doc.getFields().stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(23);
    assertThat(convertFieldBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    float conflictingFloatValue = 100.0f;
    Trace.KeyValue conflictingTagFloat =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setVType(Trace.ValueType.FLOAT32)
            .setVFloat32(conflictingFloatValue)
            .build();

    Trace.Span msg2 =
        SpanUtil.makeSpan(
            1, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagFloat));
    Document msg2Doc = convertFieldBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(31);
    String additionalCreatedFieldName = makeNewFieldOfType(conflictingFieldName, FieldType.FLOAT);
    // Value converted and new field is added.
    assertThat(
            msg2Doc.getFields().stream()
                .filter(
                    f ->
                        f.name().equals(conflictingFieldName)
                            || f.name().equals(additionalCreatedFieldName))
                .count())
        .isEqualTo(4);
    assertThat(msg2Doc.getField(conflictingFieldName).stringValue()).isEqualTo("100.0");
    assertThat(msg2Doc.getField(additionalCreatedFieldName).numericValue().floatValue())
        .isEqualTo(conflictingFloatValue);
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(24);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(conflictingFieldName, additionalCreatedFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(convertFieldBuilder.getSchema().get(additionalCreatedFieldName).fieldType)
        .isEqualTo(FieldType.FLOAT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry))
        .isEqualTo(1);
    assertThat(
            msg1Doc.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isEqualTo(1);
    assertThat(
            msg2Doc.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isEqualTo(1);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(LogMessage.SystemField.ALL.fieldName);
  }

  @Test
  public void testConversionInConvertAndDuplicateField() throws IOException {
    // TODO:
  }

  @Test
  public void testStringTextAliasing() throws JsonProcessingException {
    // TODO:
  }
}
