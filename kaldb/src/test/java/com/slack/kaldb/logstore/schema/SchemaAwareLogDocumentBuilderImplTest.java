package com.slack.kaldb.logstore.schema;

import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_AND_DUPLICATE_FIELD_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_FIELD_VALUE_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.DROP_FIELDS_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_AND_DUPLICATE_FIELD;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_FIELD_VALUE;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.RAISE_ERROR;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.build;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.makeNewFieldOfType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.kaldb.logstore.FieldDefMismatchException;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
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
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    final LogMessage message = MessageUtil.makeMessage(0);
    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(18);
    assertThat(docBuilder.getSchema().size()).isEqualTo(21);
    assertThat(docBuilder.getSchema().keySet())
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
    // Only string fields have doc values not text fields.
    assertThat(docBuilder.getSchema().get("id").fieldType.name).isEqualTo(FieldType.STRING.name);
    assertThat(
            testDocument
                .getFields()
                .stream()
                .filter(
                    f ->
                        f.name().equals(LogMessage.SystemField.ID.fieldName)
                            && f instanceof SortedDocValuesField)
                .count())
        .isEqualTo(1);
    assertThat(docBuilder.getSchema().get("index").fieldType.name).isEqualTo(FieldType.TEXT.name);
    assertThat(
            testDocument
                .getFields()
                .stream()
                .filter(f -> f.name().equals("index") && f instanceof SortedDocValuesField)
                .count())
        .isZero();
  }

  @Test
  public void testNestedDocumentCreation() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);

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
    assertThat(testDocument.getFields().size()).isEqualTo(15);
    assertThat(docBuilder.getSchema().size()).isEqualTo(21);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
                "nested.nested1",
                "nested.nested2",
                "booleanproperty"));
    assertThat(docBuilder.getSchema().get("booleanproperty").fieldType)
        .isEqualTo(FieldType.BOOLEAN);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testMaxRecursionNestedDocumentCreation() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);

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
                Map.of(
                    "nested1",
                    "value1",
                    "nested11",
                    2,
                    "nested12",
                    Map.of(
                        "nested21",
                        21,
                        "nested22",
                        Map.of("nested31", 31, "nested32", Map.of("nested41", 41))))));

    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(20);
    assertThat(docBuilder.getSchema().size()).isEqualTo(24);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
                "nested.nested1",
                "nested.nested11",
                "nested.nested12.nested21",
                "nested.nested12.nested22.nested31",
                "nested.nested12.nested22.nested32",
                "booleanproperty"));
    assertThat(docBuilder.getSchema().keySet())
        .doesNotContainAnyElementsOf(
            List.of("nested.nested12", "nested.nested12.nested22.nested32.nested41"));
    assertThat(docBuilder.getSchema().get("booleanproperty").fieldType)
        .isEqualTo(FieldType.BOOLEAN);
    assertThat(docBuilder.getSchema().get("nested.nested12.nested22.nested32").fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testMultiLevelNestedDocumentCreation() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);

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
    assertThat(testDocument.getFields().size()).isEqualTo(15);
    assertThat(docBuilder.getSchema().size()).isEqualTo(21);
    assertThat(docBuilder.getSchema().keySet())
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
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);

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
    assertThat(testDocument.getFields().size()).isEqualTo(17);
    assertThat(docBuilder.getSchema().size()).isEqualTo(23);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
                "listType",
                "nested.nested.nestedList",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(docBuilder.getSchema().get("listType").fieldType).isEqualTo(FieldType.TEXT);
    assertThat(docBuilder.getSchema().get("nested.nested.nestedList").fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testRaiseErrorOnConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(RAISE_ERROR, meterRegistry);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
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
    assertThat(msg1Doc.getFields().size()).isEqualTo(14);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(docBuilder.getSchema().size()).isEqualTo(18);
    assertThat(docBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getSchema().get(conflictingFieldName).fieldType)
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
    assertThat(docBuilder.getSchema().size()).isGreaterThanOrEqualTo(18);
    assertThat(docBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.INTEGER);

    LogMessage msg3 =
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
                123,
                "newFieldText",
                "newFieldValue"));
    assertThatThrownBy(() -> docBuilder.fromMessage(msg3))
        .isInstanceOf(FieldDefMismatchException.class);
    // NOTE: When a document indexing fails, we still register the types of the fields in this doc.
    // So, the fieldMap may contain an additional item than before.
    assertThat(docBuilder.getSchema().size()).isGreaterThanOrEqualTo(18);
    assertThat(docBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.INTEGER);
    assertThat(docBuilder.getSchema().get(LogMessage.ReservedField.HOSTNAME.fieldName).fieldType)
        .isEqualTo(FieldType.TEXT);

    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testRaiseErrorOnConflictingReservedField() {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(RAISE_ERROR, meterRegistry);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    final String hostNameField = LogMessage.ReservedField.HOSTNAME.fieldName;
    assertThat(docBuilder.getSchema().keySet()).contains(hostNameField);
    assertThat(docBuilder.getSchema().get(hostNameField).fieldType).isEqualTo(FieldType.TEXT);

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
                hostNameField,
                123));

    assertThatThrownBy(() -> docBuilder.fromMessage(msg1))
        .isInstanceOf(FieldDefMismatchException.class);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    assertThat(docBuilder.getSchema().keySet()).contains(hostNameField);
    assertThat(docBuilder.getSchema().get(hostNameField).fieldType).isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testDroppingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, meterRegistry);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
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
    assertThat(msg1Doc.getFields().size()).isEqualTo(13);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(docBuilder.getSchema().size()).isEqualTo(18);
    assertThat(docBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getSchema().get(conflictingFieldName).fieldType)
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
    assertThat(msg2Doc.getFields().size()).isEqualTo(12);
    // Conflicting field is dropped.
    assertThat(
            msg2Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isEmpty();
    assertThat(docBuilder.getSchema().size()).isEqualTo(18);
    assertThat(docBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testConvertingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        build(CONVERT_FIELD_VALUE, meterRegistry);
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(17);
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
    assertThat(msg1Doc.getFields().size()).isEqualTo(13);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(18);
    assertThat(convertFieldBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
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
    // Value is converted for conflicting field.
    assertThat(
            msg2Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(msg2Doc.getField(conflictingFieldName).stringValue()).isEqualTo("1");
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(18);
    assertThat(convertFieldBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testConvertingAndDuplicatingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        build(CONVERT_AND_DUPLICATE_FIELD, meterRegistry);
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(17);
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
    assertThat(msg1Doc.getFields().size()).isEqualTo(13);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(18);
    assertThat(convertFieldBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
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
    assertThat(msg2Doc.getFields().size()).isEqualTo(15);
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
    // Field value is null since we don't store the int field anymore.
    assertThat(msg2Doc.getField(additionalCreatedFieldName).stringValue()).isNull();
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(19);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(conflictingFieldName, additionalCreatedFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(convertFieldBuilder.getSchema().get(additionalCreatedFieldName).fieldType)
        .isEqualTo(FieldType.INTEGER);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void testValueTypeConversionWorksInDocument() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        build(CONVERT_AND_DUPLICATE_FIELD, meterRegistry);
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(17);
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
    assertThat(msg1Doc.getFields().size()).isEqualTo(13);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(18);
    assertThat(convertFieldBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
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
    assertThat(msg2Doc.getFields().size()).isEqualTo(15);
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
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(19);
    assertThat(convertFieldBuilder.getSchema().keySet())
        .contains(conflictingFieldName, additionalCreatedFieldName);
    assertThat(convertFieldBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(convertFieldBuilder.getSchema().get(additionalCreatedFieldName).fieldType)
        .isEqualTo(FieldType.FLOAT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void testConversionInConvertAndDuplicateField() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        build(CONVERT_AND_DUPLICATE_FIELD, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(CONVERT_AND_DUPLICATE_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);

    final String floatStrConflictField = "floatStrConflictField";
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
    final int expectedDocFieldsAfterMsg1 = 18;
    assertThat(testDocument1.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1);
    final int expectedFieldsAfterMsg1 = 23;
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1);
    assertThat(docBuilder.getSchema().get(floatStrConflictField).fieldType)
        .isEqualTo(FieldType.FLOAT);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
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
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
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
    assertThat(testDocument2.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1 + 1);
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1 + 1);
    assertThat(docBuilder.getSchema().get(floatStrConflictField).fieldType)
        .isEqualTo(FieldType.FLOAT);
    String additionalCreatedFieldName = makeNewFieldOfType(floatStrConflictField, FieldType.TEXT);
    assertThat(docBuilder.getSchema().get(additionalCreatedFieldName).fieldType)
        .isEqualTo(FieldType.TEXT);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
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
  }

  @Test
  public void testConversionUsingConvertField() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(CONVERT_FIELD_VALUE, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(CONVERT_FIELD_VALUE);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);

    final String floatStrConflictField = "floatStrConflictField";
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
    final int expectedDocFieldsAfterMsg1 = 18;
    assertThat(testDocument1.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1);
    final int expectedFieldsAfterMsg1 = 23;
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1);
    assertThat(docBuilder.getSchema().get(floatStrConflictField).fieldType)
        .isEqualTo(FieldType.FLOAT);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
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
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
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
    assertThat(testDocument2.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1);
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1);
    assertThat(docBuilder.getSchema().get(floatStrConflictField).fieldType)
        .isEqualTo(FieldType.FLOAT);
    String additionalCreatedFieldName = makeNewFieldOfType(floatStrConflictField, FieldType.TEXT);
    assertThat(
            testDocument2
                .getFields()
                .stream()
                .filter(f -> f.name().equals(additionalCreatedFieldName))
                .count())
        .isZero();
    assertThat(
            testDocument2
                .getFields()
                .stream()
                .filter(f -> f.name().equals(floatStrConflictField))
                .count())
        .isEqualTo(2);
    assertThat(docBuilder.getSchema().containsKey(additionalCreatedFieldName)).isFalse();
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
                floatStrConflictField,
                "nested.nested.nestedList",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(docBuilder.getSchema().containsKey(additionalCreatedFieldName)).isFalse();
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testConversionUsingDropFieldBuilder() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);

    final String floatStrConflictField = "floatStrConflictField";
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
                floatStrConflictField,
                3.0f,
                "nested",
                Map.of(
                    "leaf1",
                    "value1",
                    "nested",
                    Map.of("leaf2", "value2", "leaf21", 3, "nestedList", List.of(1)))));

    Document testDocument = docBuilder.fromMessage(message);
    final int expectedFieldsInDocumentAfterMesssage = 18;
    assertThat(testDocument.getFields().size()).isEqualTo(expectedFieldsInDocumentAfterMesssage);
    final int fieldCountAfterIndexingFirstDocument = 23;
    assertThat(docBuilder.getSchema().size()).isEqualTo(fieldCountAfterIndexingFirstDocument);
    assertThat(docBuilder.getSchema().get(floatStrConflictField).fieldType)
        .isEqualTo(FieldType.FLOAT);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
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
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
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
    assertThat(testDocument2.getFields().size())
        .isEqualTo(
            expectedFieldsInDocumentAfterMesssage - 2); // 1 dropped field, 2 less indexed fields
    assertThat(docBuilder.getSchema().size()).isEqualTo(fieldCountAfterIndexingFirstDocument);
    assertThat(docBuilder.getSchema().get(floatStrConflictField).fieldType)
        .isEqualTo(FieldType.FLOAT);
    String additionalCreatedFieldName = makeNewFieldOfType(floatStrConflictField, FieldType.TEXT);
    assertThat(
            testDocument2
                .getFields()
                .stream()
                .filter(f -> f.name().equals(additionalCreatedFieldName))
                .count())
        .isZero();
    assertThat(
            testDocument2
                .getFields()
                .stream()
                .filter(f -> f.name().equals(floatStrConflictField))
                .count())
        .isZero();
    assertThat(docBuilder.getSchema().containsKey(additionalCreatedFieldName)).isFalse();
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
                floatStrConflictField,
                "nested.nested.nestedList",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(docBuilder.getSchema().containsKey(additionalCreatedFieldName)).isFalse();
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }

  @Test
  public void testStringTextAliasing() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        build(CONVERT_AND_DUPLICATE_FIELD, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(CONVERT_AND_DUPLICATE_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);

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
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
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
    final int expectedDocFieldsAfterMsg1 = 18;
    assertThat(testDocument1.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1);
    final int expectedFieldsAfterMsg1 = 23;
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1);
    assertThat(docBuilder.getSchema().get(stringField).fieldType).isEqualTo(FieldType.STRING);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(stringField, "nested.leaf1", "nested.nested.leaf2", "nested.nested.leaf21"));
    // The new field is identified as text, but indexed as String.
    assertThat(
            testDocument1
                .getFields()
                .stream()
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
            Map.of(
                LogMessage.ReservedField.TIMESTAMP.fieldName,
                MessageUtil.getCurrentLogDate(),
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
    assertThat(testDocument2.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1 + 1);
    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1 + 1);
    assertThat(docBuilder.getSchema().get(nestedStringField).fieldType).isEqualTo(FieldType.STRING);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "@timestamp",
                stringField,
                nestedStringField,
                "nested.nested.nestedList",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
    assertThat(
            testDocument2
                .getFields()
                .stream()
                .filter(
                    f -> f.name().equals(nestedStringField) && f instanceof SortedDocValuesField)
                .count())
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
  }
}
