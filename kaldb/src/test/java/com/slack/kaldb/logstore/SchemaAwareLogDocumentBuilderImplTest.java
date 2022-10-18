package com.slack.kaldb.logstore;

import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.*;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.CONVERT_AND_DUPLICATE_FIELD_COUNTER;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.CONVERT_FIELD_VALUE_COUNTER;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.DROP_FIELDS_COUNTER;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_AND_DUPLICATE_FIELD;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_FIELD_VALUE;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.RAISE_ERROR;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.FieldType.convertFieldValue;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.makeNewFieldOfType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.junit.Before;
import org.junit.Test;

public class SchemaAwareLogDocumentBuilderImplTest {
  private SimpleMeterRegistry meterRegistry;

  @Before
  public void setup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
  }

  @Test
  public void testBasicDocumentCreation() throws IOException {
    // TODO: Add an assert for standard fields.
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(17);
    final LogMessage message = MessageUtil.makeMessage(0);
    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(16);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(21);
    assertThat(docBuilder.getFieldDefMap().keySet())
        .containsAll(
            List.of(
                "longproperty",
                "floatproperty",
                "@timestamp",
                "intproperty",
                "message",
                "doubleproperty"));
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testNestedDocumentCreation() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(17);

    LogMessage message =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                "duplicateproperty",
                "duplicate1",
                "booleanproperty",
                true,
                "nested",
                Map.of("nested1", "value1", "nested2", 2)));

    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(13);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(21);
    assertThat(docBuilder.getFieldDefMap().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
                "nested.nested1",
                "nested.nested2",
                "booleanproperty"));
    assertThat(docBuilder.getFieldDefMap().get("booleanproperty").fieldType)
        .isEqualTo(FieldType.BOOLEAN);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testMultiLevelNestedDocumentCreation() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(17);

    LogMessage message =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                "duplicateproperty",
                "duplicate1",
                "nested",
                Map.of("leaf1", "value1", "nested", Map.of("leaf2", "value2", "leaf21", 3))));

    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(13);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(21);
    assertThat(docBuilder.getFieldDefMap().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testListTypeInDocument() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        build(CONVERT_AND_DUPLICATE_FIELD, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(CONVERT_AND_DUPLICATE_FIELD);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(17);

    LogMessage message =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
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
    assertThat(testDocument.getFields().size()).isEqualTo(15);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(23);
    assertThat(docBuilder.getFieldDefMap().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
                "listType",
                "nested.nested.nestedList",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testRaiseErrorOnConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(RAISE_ERROR, meterRegistry);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(17);
    String conflictingFieldName = "conflictingField";

    LogMessage msg1 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                1));

    Document msg1Doc = docBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(12);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(18);
    assertThat(docBuilder.getFieldDefMap().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getFieldDefMap().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.INTEGER);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    LogMessage msg2 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "2",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                "newFieldText",
                "newFieldValue",
                conflictingFieldName,
                "1"));
    assertThatThrownBy(() -> docBuilder.fromMessage(msg2))
        .isInstanceOf(FieldDefMismatchException.class);
    // NOTE: When a document indexing fails, we still register the types of the fields in this doc.
    // So, the fieldMap may contain an additional item than before.
    assertThat(docBuilder.getFieldDefMap().size()).isGreaterThanOrEqualTo(18);
    assertThat(docBuilder.getFieldDefMap().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getFieldDefMap().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.INTEGER);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testDroppingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, meterRegistry);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(17);
    String conflictingFieldName = "conflictingField";

    LogMessage msg1 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                "1"));

    Document msg1Doc = docBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(11);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(18);
    assertThat(docBuilder.getFieldDefMap().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getFieldDefMap().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    LogMessage msg2 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "2",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                1));
    Document msg2Doc = docBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(10);
    // Conflicting field is dropped.
    assertThat(
            msg2Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isEmpty();
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(18);
    assertThat(docBuilder.getFieldDefMap().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getFieldDefMap().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testConvertingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        build(CONVERT_FIELD_VALUE, meterRegistry);
    assertThat(convertFieldBuilder.getFieldDefMap().size()).isEqualTo(17);
    String conflictingFieldName = "conflictingField";

    LogMessage msg1 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                "1"));

    Document msg1Doc = convertFieldBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(11);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(convertFieldBuilder.getFieldDefMap().size()).isEqualTo(18);
    assertThat(convertFieldBuilder.getFieldDefMap().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getFieldDefMap().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    LogMessage msg2 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "2",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                1));
    Document msg2Doc = convertFieldBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(11);
    // Value is converted for conflicting field.
    assertThat(
            msg2Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(msg2Doc.getField(conflictingFieldName).stringValue()).isEqualTo("1");
    assertThat(convertFieldBuilder.getFieldDefMap().size()).isEqualTo(18);
    assertThat(convertFieldBuilder.getFieldDefMap().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getFieldDefMap().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testConvertingAndDuplicatingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        build(CONVERT_AND_DUPLICATE_FIELD, meterRegistry);
    assertThat(convertFieldBuilder.getFieldDefMap().size()).isEqualTo(17);
    String conflictingFieldName = "conflictingField";

    LogMessage msg1 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                "1"));

    Document msg1Doc = convertFieldBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(11);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(convertFieldBuilder.getFieldDefMap().size()).isEqualTo(18);
    assertThat(convertFieldBuilder.getFieldDefMap().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getFieldDefMap().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    LogMessage msg2 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "2",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                1));
    Document msg2Doc = convertFieldBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(13);
    String additionalCreatedFieldName = makeNewFieldOfType(conflictingFieldName, FieldType.INTEGER);
    // Value converted and new field is added.
    assertThat(
            msg2Doc
                .getFields()
                .stream()
                .filter(
                    f ->
                        f.name().equals(conflictingFieldName)
                            || f.name().equals(additionalCreatedFieldName))
                .count())
        .isEqualTo(3);
    assertThat(msg2Doc.getField(conflictingFieldName).stringValue()).isEqualTo("1");
    assertThat(msg2Doc.getField(additionalCreatedFieldName).stringValue()).isEqualTo("1");
    assertThat(convertFieldBuilder.getFieldDefMap().size()).isEqualTo(19);
    assertThat(convertFieldBuilder.getFieldDefMap().keySet())
        .contains(conflictingFieldName, additionalCreatedFieldName);
    assertThat(convertFieldBuilder.getFieldDefMap().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(convertFieldBuilder.getFieldDefMap().get(additionalCreatedFieldName).fieldType)
        .isEqualTo(FieldType.INTEGER);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void testValueTypeConversionWorks() {
    assertThat(convertFieldValue("1", FieldType.TEXT, FieldType.INTEGER)).isEqualTo(1);
    assertThat(convertFieldValue("1", FieldType.TEXT, FieldType.LONG)).isEqualTo(1L);
    assertThat(convertFieldValue("2", FieldType.TEXT, FieldType.FLOAT)).isEqualTo(2.0);
    assertThat(convertFieldValue("3", FieldType.TEXT, FieldType.DOUBLE)).isEqualTo(3.0);

    int intValue = 1;
    assertThat(convertFieldValue(intValue, FieldType.INTEGER, FieldType.TEXT)).isEqualTo("1");
    assertThat(convertFieldValue(intValue + 1, FieldType.INTEGER, FieldType.LONG)).isEqualTo(2L);
    assertThat(convertFieldValue(intValue + 2, FieldType.INTEGER, FieldType.FLOAT)).isEqualTo(3.0f);
    assertThat(convertFieldValue(intValue + 3, FieldType.INTEGER, FieldType.DOUBLE)).isEqualTo(4.0);

    long longValue = 1L;
    assertThat(convertFieldValue(longValue, FieldType.LONG, FieldType.TEXT)).isEqualTo("1");
    assertThat(convertFieldValue(longValue + 1, FieldType.LONG, FieldType.INTEGER)).isEqualTo(2);
    assertThat(convertFieldValue(longValue + 2, FieldType.LONG, FieldType.FLOAT)).isEqualTo(3.0f);
    assertThat(convertFieldValue(longValue + 3, FieldType.LONG, FieldType.DOUBLE)).isEqualTo(4.0);

    float floatValue = 1.0f;
    assertThat(convertFieldValue(floatValue, FieldType.FLOAT, FieldType.TEXT)).isEqualTo("1.0");
    assertThat(convertFieldValue(floatValue + 1.0f, FieldType.FLOAT, FieldType.INTEGER))
        .isEqualTo(2);
    assertThat(convertFieldValue(floatValue + 2.0f, FieldType.FLOAT, FieldType.LONG)).isEqualTo(3L);
    assertThat(convertFieldValue(floatValue + 3.0f, FieldType.FLOAT, FieldType.DOUBLE))
        .isEqualTo(4.0);

    double doubleValue = 1.0;
    assertThat(convertFieldValue(doubleValue, FieldType.DOUBLE, FieldType.TEXT)).isEqualTo("1.0");
    assertThat(convertFieldValue(doubleValue + 1.0f, FieldType.DOUBLE, FieldType.INTEGER))
        .isEqualTo(2);
    assertThat(convertFieldValue(doubleValue + 2.0f, FieldType.DOUBLE, FieldType.LONG))
        .isEqualTo(3L);
    assertThat(convertFieldValue(doubleValue + 3.0f, FieldType.DOUBLE, FieldType.FLOAT))
        .isEqualTo(4.0f);
  }

  @Test
  public void testValueTypeConversionWorksInDocument() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        build(CONVERT_AND_DUPLICATE_FIELD, meterRegistry);
    assertThat(convertFieldBuilder.getFieldDefMap().size()).isEqualTo(17);
    String conflictingFieldName = "conflictingField";

    LogMessage msg1 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "1",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                "1"));

    Document msg1Doc = convertFieldBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(11);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(convertFieldBuilder.getFieldDefMap().size()).isEqualTo(18);
    assertThat(convertFieldBuilder.getFieldDefMap().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getFieldDefMap().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    float conflictingFloatValue = 100.0f;
    LogMessage msg2 =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "INFO",
            "2",
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
                LogMessage.ReservedField.MESSAGE.fieldName,
                "Test message",
                LogMessage.ReservedField.TAG.fieldName,
                "foo-bar",
                LogMessage.ReservedField.HOSTNAME.fieldName,
                "host1-dc2.abc.com",
                conflictingFieldName,
                conflictingFloatValue));
    Document msg2Doc = convertFieldBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(13);
    String additionalCreatedFieldName = makeNewFieldOfType(conflictingFieldName, FieldType.FLOAT);
    // Value converted and new field is added.
    assertThat(
            msg2Doc
                .getFields()
                .stream()
                .filter(
                    f ->
                        f.name().equals(conflictingFieldName)
                            || f.name().equals(additionalCreatedFieldName))
                .count())
        .isEqualTo(3);
    assertThat(msg2Doc.getField(conflictingFieldName).stringValue()).isEqualTo("100.0");
    assertThat(msg2Doc.getField(additionalCreatedFieldName).numericValue().floatValue())
        .isEqualTo(conflictingFloatValue);
    assertThat(convertFieldBuilder.getFieldDefMap().size()).isEqualTo(19);
    assertThat(convertFieldBuilder.getFieldDefMap().keySet())
        .contains(conflictingFieldName, additionalCreatedFieldName);
    assertThat(convertFieldBuilder.getFieldDefMap().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(convertFieldBuilder.getFieldDefMap().get(additionalCreatedFieldName).fieldType)
        .isEqualTo(FieldType.FLOAT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry))
        .isEqualTo(1);
  }

  // TODO: Add a test to assert fields of reserved fields can't be changed.
}
