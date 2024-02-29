package com.slack.kaldb.logstore.schema;

import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_AND_DUPLICATE_FIELD_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_FIELD_VALUE_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.DROP_FIELDS_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.build;
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
    Document testDocument = docBuilder.fromMessage(SpanUtil.makeSpan(0));
    assertThat(testDocument.getFields().size()).isEqualTo(23);
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
    Document testDocument = docBuilder.fromMessage(SpanUtil.makeSpan(0));
    assertThat(testDocument.getFields().size()).isEqualTo(22);
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
    // TODO:
  }

  @Test
  public void testMaxRecursionNestedDocumentCreation() throws IOException {
    // TODO:
  }

  @Test
  public void testMultiLevelNestedDocumentCreation() throws IOException {
    // TODO:
  }

  @Test
  public void testMultiLevelNestedDocumentCreationWithoutFulltTextSearch() throws IOException {
    // TODO:
  }

  @Test
  public void testDroppingConflictingField() throws JsonProcessingException {
    SchemaAwareLogDocumentBuilderImpl docBuilder = build(DROP_FIELD, true, meterRegistry);
    assertThat(docBuilder.getSchema().size()).isEqualTo(17);
    assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
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

    Trace.KeyValue conflictingTagInt =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setVType(Trace.ValueType.INT32)
            .setVInt32(1)
            .build();

    Trace.Span msg1 =
        SpanUtil.makeSpan(
            1, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagStr));

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
        .isEqualTo(FieldType.STRING);
    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isZero();
    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER, meterRegistry)).isZero();

    Trace.Span msg2 =
        SpanUtil.makeSpan(
            1, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagInt));
    Document msg2Doc = docBuilder.fromMessage(msg2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(27);
    // Conflicting field is dropped.
    assertThat(
            msg2Doc.getFields().stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isEmpty();
    assertThat(docBuilder.getSchema().size()).isEqualTo(23);
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
    // TODO:
  }
}
