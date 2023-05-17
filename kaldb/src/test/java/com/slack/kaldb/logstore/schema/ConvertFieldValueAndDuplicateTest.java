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
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertFieldValueAndDuplicateTest {

  private SimpleMeterRegistry meterRegistry;
  private static final Logger LOG = LoggerFactory.getLogger(RaiseErrorFieldValueTest.class);

  @Before
  public void setup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
  }

  @Test
  public void testListTypeInDocument() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        build(CONVERT_VALUE_AND_DUPLICATE_FIELD, true, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy())
        .isEqualTo(CONVERT_VALUE_AND_DUPLICATE_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);

    LogMessage message =
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
                "listType",
                Collections.emptyList(),
                "nested",
                Map.of(
                    "leaf1",
                    "value1",
                    "nested",
                    Map.of("leaf2", "value2", "leaf21", 3, "nestedList", List.of(1)))));

    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(22);
    assertThat(docBuilder.getSchema().size()).isEqualTo(23);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "listType",
                "nested.nested.nestedList",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(docBuilder.getSchema().get("listType").fieldType).isEqualTo(FieldType.STRING);
    assertThat(docBuilder.getSchema().get("nested.nested.nestedList").fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
    assertThat(
            testDocument.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isEqualTo(1);
  }

  @Test
  public void testListTypeInDocumentWithoutFullTextSearch() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        build(CONVERT_VALUE_AND_DUPLICATE_FIELD, false, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy())
        .isEqualTo(CONVERT_VALUE_AND_DUPLICATE_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(16);
    assertThat(docBuilder.getSchema().keySet())
        .doesNotContain(LogMessage.SystemField.ALL.fieldName);

    LogMessage message =
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
                "listType",
                Collections.emptyList(),
                "nested",
                Map.of(
                    "leaf1",
                    "value1",
                    "nested",
                    Map.of("leaf2", "value2", "leaf21", 3, "nestedList", List.of(1)))));

    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(21);
    assertThat(docBuilder.getSchema().size()).isEqualTo(22);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "listType",
                "nested.nested.nestedList",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(docBuilder.getSchema().get("listType").fieldType).isEqualTo(FieldType.STRING);
    assertThat(docBuilder.getSchema().get("nested.nested.nestedList").fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
    assertThat(docBuilder.getSchema().keySet())
        .doesNotContain(LogMessage.SystemField.ALL.fieldName);
    assertThat(
            testDocument.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isZero();
  }

  @Test
  public void testConvertingAndDuplicatingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        build(CONVERT_VALUE_AND_DUPLICATE_FIELD, true, meterRegistry);
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

    Document msg1Doc = convertFieldBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(14);
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
    Document msg2Doc = convertFieldBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(16);
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
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(19);
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
                true));

    Document msg1Doc = convertFieldBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(14);
    assertThat(
            msg1Doc.getFields().stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(18);
    assertThat(convertFieldBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.BOOLEAN);
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
                "random"));
    Document msg2Doc = convertFieldBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(16);
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
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(19);
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
    LogMessage msg3 =
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
                additionalCreatedFieldName,
                true));
    Document msg3Doc = convertFieldBuilder.fromMessage(msg3);
    assertThat(msg3Doc.getFields().size()).isEqualTo(16);
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
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(20);
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

    Document msg1Doc = convertFieldBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(14);
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

    float conflictingFloatValue = 100.0f;
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
                conflictingFloatValue));
    Document msg2Doc = convertFieldBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(16);
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
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(19);
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
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        build(CONVERT_VALUE_AND_DUPLICATE_FIELD, true, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy())
        .isEqualTo(CONVERT_VALUE_AND_DUPLICATE_FIELD);
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

    Document testDocument1 = docBuilder.fromMessage(msg1);
    final int expectedDocFieldsAfterMsg1 = 22;
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
    Document testDocument2 = docBuilder.fromMessage(msg2);
    assertThat(testDocument2.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1 + 2);
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1 + 1);
    assertThat(docBuilder.getSchema().get(floatStrConflictField).fieldType)
        .isEqualTo(FieldType.FLOAT);
    String additionalCreatedFieldName = makeNewFieldOfType(floatStrConflictField, FieldType.STRING);
    assertThat(docBuilder.getSchema().get(additionalCreatedFieldName).fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                floatStrConflictField,
                additionalCreatedFieldName,
                "nested.nested.nestedList",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry))
        .isEqualTo(1);
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

  @Test
  public void testStringTextAliasing() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        build(CONVERT_VALUE_AND_DUPLICATE_FIELD, true, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy())
        .isEqualTo(CONVERT_VALUE_AND_DUPLICATE_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);

    // Set stringField is a String
    final String stringField = "stringField";
    docBuilder
        .getSchema()
        .put(
            stringField, new LuceneFieldDef(stringField, FieldType.STRING.name, false, true, true));
    assertThat(docBuilder.getSchema().size()).isEqualTo(18);

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
                stringField,
                "strFieldValue",
                "nested",
                Map.of(
                    "leaf1",
                    "value1",
                    "nested",
                    Map.of("leaf2", "value2", "leaf21", 3, "nestedList", List.of(1)))));

    Document testDocument1 = docBuilder.fromMessage(msg1);
    final int expectedDocFieldsAfterMsg1 = 22;
    assertThat(testDocument1.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1);
    final int expectedFieldsAfterMsg1 = 23;
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1);
    assertThat(docBuilder.getSchema().get(stringField).fieldType).isEqualTo(FieldType.STRING);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(stringField, "nested.leaf1", "nested.nested.leaf2", "nested.nested.leaf21"));
    // The new field is identified as text, but indexed as String.
    assertThat(
            testDocument1.getFields().stream()
                .filter(f -> f.name().equals(stringField) && f instanceof SortedDocValuesField)
                .count())
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    // Set stringField in a nested field
    final String nestedStringField = "nested.nested.stringField";
    docBuilder
        .getSchema()
        .put(
            nestedStringField,
            new LuceneFieldDef(nestedStringField, FieldType.STRING.name, false, true, true));
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1 + 1);

    LogMessage msg2 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "2",
            Instant.now(),
            Map.of(
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message2",
                "duplicateproperty",
                "duplicate1",
                stringField,
                "strFieldValue2",
                "nested",
                Map.of(
                    "leaf1",
                    "value1",
                    "nested",
                    Map.of(
                        stringField, "nestedStringField", "leaf21", 3, "nestedList", List.of(1)))));

    Document testDocument2 = docBuilder.fromMessage(msg2);
    // Nested string field adds 1 more sorted doc values field.
    assertThat(testDocument2.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1);
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1 + 1);
    assertThat(docBuilder.getSchema().get(nestedStringField).fieldType).isEqualTo(FieldType.STRING);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                stringField,
                nestedStringField,
                "nested.nested.nestedList",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(
            testDocument2.getFields().stream()
                .filter(
                    f -> f.name().equals(nestedStringField) && f instanceof SortedDocValuesField)
                .count())
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
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
