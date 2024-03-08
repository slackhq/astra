package com.slack.kaldb.logstore.schema;

import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_AND_DUPLICATE_FIELD_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.CONVERT_FIELD_VALUE_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.DROP_FIELDS_COUNTER;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_FIELD_VALUE;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.build;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
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
    assertThat(msg2Doc.getFields().size()).isEqualTo(29);
    // Value is converted for conflicting field.
    assertThat(
            msg2Doc.getFields().stream()
                .filter(f -> f.name().equals(conflictingFieldName))
                .findFirst())
        .isNotEmpty();
    assertThat(msg2Doc.getField(conflictingFieldName).stringValue()).isEqualTo("1");
    assertThat(convertFieldBuilder.getSchema().size()).isEqualTo(23);
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

    Trace.Span span =
        Trace.Span.newBuilder()
            .setId(ByteString.copyFromUtf8("1"))
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVStr("Test message")
                    .setKey(LogMessage.ReservedField.MESSAGE.fieldName)
                    .setVType(Trace.ValueType.STRING)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVStr("duplicate1")
                    .setKey("duplicateproperty")
                    .setVType(Trace.ValueType.STRING)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVFloat32(3.0f)
                    .setKey(floatStrConflictField)
                    .setVType(Trace.ValueType.FLOAT32)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVStr("value1")
                    .setKey("nested.leaf1")
                    .setVType(Trace.ValueType.STRING)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVStr("value2")
                    .setKey("nested.nested.leaf1")
                    .setVType(Trace.ValueType.STRING)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVInt32(3)
                    .setKey("nested.nested.leaf21")
                    .setVType(Trace.ValueType.INT32)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVInt32(1)
                    .setKey("nested.nested.nestedList")
                    .setVType(Trace.ValueType.INT32)
                    .build())
            .build();

    Document testDocument1 = docBuilder.fromMessage(span);
    final int expectedDocFieldsAfterMsg1 = 25;
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

    //    LogMessage msg2 =
    //            new LogMessage(
    //                    MessageUtil.TEST_DATASET_NAME,
    //                    "INFO",
    //                    "1",
    //                    Instant.now(),
    //                    Map.of(
    //                            LogMessage.ReservedField.MESSAGE.fieldName,
    //                            "Test message",
    //                            "duplicateproperty",
    //                            "duplicate1",
    //                            floatStrConflictField,
    //                            "blah",
    //                            "nested",
    //                            Map.of(
    //                                    "leaf1",
    //                                    "value1",
    //                                    "nested",
    //                                    Map.of("leaf2", "value2", "leaf21", 3, "nestedList",
    // List.of(1)))));
    //    Document testDocument2 = docBuilder.fromMessage(msg2);
    //    assertThat(testDocument2.getFields().size()).isEqualTo(expectedDocFieldsAfterMsg1);
    //    assertThat(docBuilder.getSchema().size()).isEqualTo(expectedFieldsAfterMsg1);
    //    assertThat(docBuilder.getSchema().get(floatStrConflictField).fieldType)
    //            .isEqualTo(FieldType.FLOAT);
    //    String additionalCreatedFieldName = makeNewFieldOfType(floatStrConflictField,
    // FieldType.TEXT);
    //    assertThat(
    //            testDocument2.getFields().stream()
    //                    .filter(f -> f.name().equals(additionalCreatedFieldName))
    //                    .count())
    //            .isZero();
    //    assertThat(
    //            testDocument2.getFields().stream()
    //                    .filter(f -> f.name().equals(floatStrConflictField))
    //                    .count())
    //            .isEqualTo(2);
    //    assertThat(docBuilder.getSchema().containsKey(additionalCreatedFieldName)).isFalse();
    //    assertThat(docBuilder.getSchema().keySet())
    //            .containsAll(
    //                    List.of(
    //                            "duplicateproperty",
    //                            floatStrConflictField,
    //                            "nested.nested.nestedList",
    //                            "nested.leaf1",
    //                            "nested.nested.leaf2",
    //                            "nested.nested.leaf21"));
    //    assertThat(docBuilder.getSchema().containsKey(additionalCreatedFieldName)).isFalse();
    //    assertThat(MetricsUtil.getCount(DROP_FIELDS_COUNTER, meterRegistry)).isZero();
    //    assertThat(MetricsUtil.getCount(CONVERT_FIELD_VALUE_COUNTER, meterRegistry)).isEqualTo(1);
    //    assertThat(MetricsUtil.getCount(CONVERT_AND_DUPLICATE_FIELD_COUNTER,
    // meterRegistry)).isZero();
    //    assertThat(
    //            testDocument1.getFields().stream()
    //                    .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
    //                    .count())
    //            .isEqualTo(1);
    //    assertThat(
    //            testDocument2.getFields().stream()
    //                    .filter(f -> f.name().equals(LogMessage.SystemField.ALL.fieldName))
    //                    .count())
    //            .isEqualTo(1);
    //
    // assertThat(docBuilder.getSchema().keySet()).contains(LogMessage.SystemField.ALL.fieldName);
  }
}
