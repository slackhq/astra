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
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaiseErrorFieldValueTest {
  private SimpleMeterRegistry meterRegistry;
  private static final Logger LOG = LoggerFactory.getLogger(RaiseErrorFieldValueTest.class);

  @Before
  public void setup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
  }

  @Test
  public void testRaiseErrorOnConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(RAISE_ERROR, true, meterRegistry);
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
            Instant.now(),
            Map.of(
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
            Instant.now(),
            Map.of(
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
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
    assertThat(
            msg1Doc
                .getFields()
                .stream()
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
    assertThat(docBuilder.getSchema().get(hostNameField).fieldType).isEqualTo(FieldType.TEXT);

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
    assertThat(docBuilder.getSchema().get(hostNameField).fieldType).isEqualTo(FieldType.TEXT);

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
                hostNameField,
                123));

    assertThatThrownBy(() -> docBuilder.fromMessage(msg1))
        .isInstanceOf(FieldDefMismatchException.class);
    assertThat(docBuilder.getSchema().size()).isEqualTo(16);
    assertThat(docBuilder.getSchema().keySet()).contains(hostNameField);
    assertThat(docBuilder.getSchema().get(hostNameField).fieldType).isEqualTo(FieldType.TEXT);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();
    assertThat(docBuilder.getSchema().keySet())
        .doesNotContain(LogMessage.SystemField.ALL.fieldName);
  }
}
