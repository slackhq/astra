package com.slack.kaldb.logstore;

import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_AND_DUPLICATE_FIELD;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_FIELD_VALUE;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.RAISE_ERROR;
import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.makeNewFieldOfType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.kaldb.testlib.MessageUtil;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.junit.Test;

public class SchemaAwareLogDocumentBuilderImplTest {

  @Test
  public void testBasicDocumentCreation() throws IOException {
    // TODO: Add an assert for standard fields.
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(DROP_FIELD);
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
  }

  // TODO: Add a test for nested field with same name as top level field.
  // TODO: Add a test for duplicate field in the map.

  @Test
  public void testNestedDocumentCreation() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(DROP_FIELD);
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
                Map.of("nested1", "value1", "nested2", 2)));

    Document testDocument = docBuilder.fromMessage(message);
    assertThat(testDocument.getFields().size()).isEqualTo(12);
    assertThat(docBuilder.getFieldDefMap().size()).isEqualTo(20);
    assertThat(docBuilder.getFieldDefMap().keySet())
        .containsAll(
            List.of("duplicateproperty", "@timestamp", "nested.nested1", "nested.nested2"));
  }

  @Test
  public void testMultiLevelNestedDocumentCreation() throws IOException {
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(DROP_FIELD);
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
  }

  @Test
  public void testRaiseErrorOnConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(RAISE_ERROR);
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
    assertThat(docBuilder.getFieldDefMap().get(conflictingFieldName).type)
        .isEqualTo(SchemaAwareLogDocumentBuilderImpl.PropertyType.INTEGER);

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
        .isInstanceOf(PropertyTypeMismatchException.class);
    // NOTE: When a document indexing fails, we still register the types of the fields in this doc.
    // So, the fieldMap may contain an additional item than before.
    assertThat(docBuilder.getFieldDefMap().size()).isGreaterThanOrEqualTo(18);
    assertThat(docBuilder.getFieldDefMap().keySet()).contains(conflictingFieldName);
    assertThat(docBuilder.getFieldDefMap().get(conflictingFieldName).type)
        .isEqualTo(SchemaAwareLogDocumentBuilderImpl.PropertyType.INTEGER);
  }

  @Test
  public void testDroppingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(DROP_FIELD);
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
    assertThat(docBuilder.getFieldDefMap().get(conflictingFieldName).type)
        .isEqualTo(SchemaAwareLogDocumentBuilderImpl.PropertyType.TEXT);

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
    assertThat(docBuilder.getFieldDefMap().get(conflictingFieldName).type)
        .isEqualTo(SchemaAwareLogDocumentBuilderImpl.PropertyType.TEXT);
  }

  @Test
  public void testConvertingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(CONVERT_FIELD_VALUE);
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
    assertThat(convertFieldBuilder.getFieldDefMap().get(conflictingFieldName).type)
        .isEqualTo(SchemaAwareLogDocumentBuilderImpl.PropertyType.TEXT);

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
    assertThat(convertFieldBuilder.getFieldDefMap().get(conflictingFieldName).type)
        .isEqualTo(SchemaAwareLogDocumentBuilderImpl.PropertyType.TEXT);
  }

  @Test
  public void testConvertingAndDuplicatingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl convertFieldBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(CONVERT_AND_DUPLICATE_FIELD);
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
    assertThat(convertFieldBuilder.getFieldDefMap().get(conflictingFieldName).type)
        .isEqualTo(SchemaAwareLogDocumentBuilderImpl.PropertyType.TEXT);

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
    String additionalCreatedFieldName =
        makeNewFieldOfType(
            conflictingFieldName, SchemaAwareLogDocumentBuilderImpl.PropertyType.INTEGER);
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
    assertThat(convertFieldBuilder.getFieldDefMap().get(conflictingFieldName).type)
        .isEqualTo(SchemaAwareLogDocumentBuilderImpl.PropertyType.TEXT);
    assertThat(convertFieldBuilder.getFieldDefMap().get(additionalCreatedFieldName).type)
        .isEqualTo(SchemaAwareLogDocumentBuilderImpl.PropertyType.INTEGER);
  }

  // TODO: Add a unit test for messages with field conflicts and handling
  // TODO: Add every type to every type change.
}
