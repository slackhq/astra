package com.slack.kaldb.logstore.schema;

import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_AND_DUPLICATE_FIELD_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_FIELD_VALUE_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.DROP_FIELDS_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_FIELD_VALUE;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.build;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.makeNewFieldOfType;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConvertFieldValueTest {

  private SimpleMeterRegistry meterRegistry;

  @BeforeEach
  public void setup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
  }

  @Test
  public void testConvertingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        build(CONVERT_FIELD_VALUE, true, meterRegistry);
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(17);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(LogMessage.SystemField.ALL.fieldName);
    String conflictingFieldName = "conflictingField";

    LogMessage msg1 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Instant.now(),
            Map.of(
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                "1"));

    Document msg1Doc = convertFieldBuilder.fromMessage(MessageUtil.convertLogMessageToSpan(msg1));
    assertThat(msg1Doc.getFields().size()).isEqualTo(17);
    assertThat(
            msg1Doc.getFields().stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(18);
    assertThat(convertFieldBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    LogMessage msg2 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "2",
            Instant.now(),
            Map.of(
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                1));
    Document msg2Doc = convertFieldBuilder.fromMessage(MessageUtil.convertLogMessageToSpan(msg2));
    assertThat(msg2Doc.getFields().size()).isEqualTo(17);
    // Value is converted for conflicting field.
    assertThat(
            msg2Doc.getFields().stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(msg2Doc.getField(conflictingFieldName).stringValue()).isEqualTo("1");
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(18);
    assertThat(convertFieldBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
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
  public void testConversionUsingConvertField() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(CONVERT_FIELD_VALUE, true, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(CONVERT_FIELD_VALUE);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);

    final String floatStrConflictField = "floatStrConflictField";
    LogMessage msg1 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Instant.now(),
            Map.of(
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                "duplicateproperty",
                "duplicate1",
                floatStrConflictField,
                3.0f,
                "nested",
                Map.of(
                    "leaf1",
                    "value1",
                    "nested",
                    Map.of("leaf2", "value2", "leaf21", 3, "nestedList", List.of(1)))));

    Document testDocument1 = docBuilder.fromMessage(MessageUtil.convertLogMessageToSpan(msg1));
    final int expectedDocFieldsAfterMsg1 = 23;
    assertThat(testDocument1.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1);
    final int expectedFieldsAfterMsg1 = 23;
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1);
    assertThat(docBuilder.getSchema().get(floatStrConflictField).fieldType)
        .isEqualTo(FieldType.FLOAT);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                floatStrConflictField,
                "nested.nested.nestedList",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    LogMessage msg2 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Instant.now(),
            Map.of(
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                "duplicateproperty",
                "duplicate1",
                floatStrConflictField,
                "blah",
                "nested",
                Map.of(
                    "leaf1",
                    "value1",
                    "nested",
                    Map.of("leaf2", "value2", "leaf21", 3, "nestedList", List.of(1)))));
    Document testDocument2 = docBuilder.fromMessage(MessageUtil.convertLogMessageToSpan(msg2));
    assertThat(testDocument2.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1);
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1);
    assertThat(docBuilder.getSchema().get(floatStrConflictField).fieldType)
        .isEqualTo(FieldType.FLOAT);
    String additionalCreatedFieldName = makeNewFieldOfType(floatStrConflictField, FieldType.TEXT);
    assertThat(
            testDocument2.getFields().stream()
                .filter(f -> f.name().equals(additionalCreatedFieldName))
                .count())
        .isZero();
    assertThat(
            testDocument2.getFields().stream()
                .filter(f -> f.name().equals(floatStrConflictField))
                .count())
        .isEqualTo(2);
    assertThat(docBuilder.getSchema().containsKey(additionalCreatedFieldName)).isFalse();
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                floatStrConflictField,
                "nested.nested.nestedList",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(docBuilder.getSchema().containsKey(additionalCreatedFieldName)).isFalse();
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
    assertThat(
            testDocument1.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isEqualTo(1);
    assertThat(
            testDocument2.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isEqualTo(1);
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
  }
}
