package com.slack.kaldb.logstore.schema;

import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_AND_DUPLICATE_FIELD_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_FIELD_VALUE_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.DROP_FIELDS_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD;
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
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DropPolicyTest {

  private SimpleMeterRegistry meterRegistry;

  @BeforeEach
  public void setup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
  }

  @Test
  public void testBasicDocumentCreation() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, true, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
    final LogMessage message = MessageUtil.makeMessage(0);
    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(21);
    assertThat(docBuilder.getSchema().size()).isEqualTo(22);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of("longproperty", "floatproperty", "intproperty", "message", "doubleproperty"));
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
    // Only string fields have doc values not text fields.
    assertThat(docBuilder.getSchema().get("_id").fieldType.name).isEqualTo(FieldType.ID.name);
    assertThat(
            testDocument.getFields().stream()
                .filter(
                    f ->
                        f.name().equals(LogMessage.SystemField.ID.fieldName)
                            && f instanceof SortedDocValuesField)
                .count())
        .isEqualTo(1);
    assertThat(docBuilder.getSchema().get("_index").fieldType.name)
        .isEqualTo(FieldType.STRING.name);
    assertThat(
            testDocument.getFields().stream()
                .filter(f -> f.name().equals("_index") && f instanceof SortedDocValuesField)
                .count())
        .isEqualTo(1);
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
    assertThat(
            testDocument.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isEqualTo(1);
    // Ensure lucene field name and the name in schema match.
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            docBuilder.getSchema().values().stream().map(f -> f.name).collect(Collectors.toList()));
  }

  @Test
  public void testBasicDocumentCreationWithoutFullTextSearch() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, false, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(16);
    assertThat(docBuilder.getSchema().keySet())
        .doesNotContain(LogMessage.SystemField.ALL.fieldName);
    final LogMessage message = MessageUtil.makeMessage(0);
    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(20);
    assertThat(docBuilder.getSchema().size()).isEqualTo(21);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of("longproperty", "floatproperty", "intproperty", "message", "doubleproperty"));
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
    // Only string fields have doc values not text fields.
    assertThat(docBuilder.getSchema().get("_id").fieldType.name).isEqualTo(FieldType.ID.name);
    assertThat(
            testDocument.getFields().stream()
                .filter(
                    f ->
                        f.name().equals(LogMessage.SystemField.ID.fieldName)
                            && f instanceof SortedDocValuesField)
                .count())
        .isEqualTo(1);
    assertThat(docBuilder.getSchema().get("_index").fieldType.name)
        .isEqualTo(FieldType.STRING.name);
    assertThat(
            testDocument.getFields().stream()
                .filter(f -> f.name().equals("_index") && f instanceof SortedDocValuesField)
                .count())
        .isEqualTo(1);
    assertThat(docBuilder.getSchema().keySet())
        .doesNotContain(LogMessage.SystemField.ALL.fieldName);
    assertThat(
            testDocument.getFields().stream()
                .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
                .count())
        .isZero();
    // Ensure lucene field name and the name in schema match.
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            docBuilder.getSchema().values().stream().map(f -> f.name).collect(Collectors.toList()));
  }

  @Test
  public void testNestedDocumentCreation() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, true, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
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
                "booleanproperty",
                true,
                "nested",
                Map.of("nested1", "value1", "nested2", 2)));

    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(19);
    assertThat(docBuilder.getSchema().size()).isEqualTo(21);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of("duplicateproperty", "nested.nested1", "nested.nested2", "booleanproperty"));
    assertThat(docBuilder.getSchema().get("booleanproperty").fieldType)
        .isEqualTo(FieldType.BOOLEAN);
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
  public void testMaxRecursionNestedDocumentCreation() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, true, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);

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
    assertThat(testDocument.getFields().size()).isEqualTo(25);
    assertThat(docBuilder.getSchema().size()).isEqualTo(24);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
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
  public void testMultiLevelNestedDocumentCreation() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, true, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
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
                "nested",
                Map.of("leaf1", "value1", "nested", Map.of("leaf2", "value2", "leaf21", 3))));

    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(19);
    assertThat(docBuilder.getSchema().size()).isEqualTo(21);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
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
  public void testMultiLevelNestedDocumentCreationWithoutFulltTextSearch() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, false, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
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
                "nested",
                Map.of("leaf1", "value1", "nested", Map.of("leaf2", "value2", "leaf21", 3))));

    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(18);
    assertThat(docBuilder.getSchema().size()).isEqualTo(20);
    assertThat(docBuilder.getSchema().keySet())
        .containsAll(
            List.of(
                "duplicateproperty",
                "nested.leaf1",
                "nested.nested.leaf2",
                "nested.nested.leaf21"));
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
  public void testDroppingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, true, meterRegistry);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
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

    Document msg1Doc = docBuilder.fromMessage(msg1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(17);
    assertThat(
            msg1Doc.getFields().stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(docBuilder.getSchema().size()).isEqualTo(18);
    assertThat(docBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getSchema().get(conflictingFieldName).fieldType)
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
    Document msg2Doc = docBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(15);
    // Conflicting field is dropped.
    assertThat(
            msg2Doc.getFields().stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isEmpty();
    assertThat(docBuilder.getSchema().size()).isEqualTo(18);
    assertThat(docBuilder.getSchema().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getSchema().get(conflictingFieldName).fieldType)
        .isEqualTo(FieldType.STRING);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
  }

  @Test
  public void testConversionUsingDropFieldBuilder() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, true, meterRegistry);
    assertThat(docBuilder.getIndexFieldConflictPolicy()).isEqualTo(DROP_FIELD);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);

    final String floatStrConflictField = "floatStrConflictField";
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
                floatStrConflictField,
                3.0f,
                "nested",
                Map.of(
                    "leaf1",
                    "value1",
                    "nested",
                    Map.of("leaf2", "value2", "leaf21", 3, "nestedList", List.of(1)))));

    Document testDocument = docBuilder.fromMessage(message);
    final int expectedFieldsInDocumentAfterMesssage = 23;
    assertThat(testDocument.getFields().size()).isEqualTo(expectedFieldsInDocumentAfterMesssage);
    final int fieldCountAfterIndexingFirstDocument = 23;
    assertThat(docBuilder.getSchema().size()).isEqualTo(fieldCountAfterIndexingFirstDocument);
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
    assertThat(testDocument2.getFields().size())
        .isEqualTo(
            expectedFieldsInDocumentAfterMesssage - 2); // 1 dropped field, 2 less indexed fields
    assertThat(docBuilder.getSchema().size()).isEqualTo(fieldCountAfterIndexingFirstDocument);
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
        .isZero();
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
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
    assertThat(
            testDocument.getFields().stream()
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
