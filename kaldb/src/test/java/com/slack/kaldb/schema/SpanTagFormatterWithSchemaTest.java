package com.slack.kaldb.schema;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.slack.kaldb.proto.schema.Schema;
import com.slack.kaldb.writer.SpanFormatter;
import com.slack.service.murron.trace.Trace;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SpanTagFormatterWithSchemaTest {

  static Schema.IngestSchema schema;

  @BeforeAll
  public static void initializeSchema() {
    Map<String, Schema.SchemaField> fields = new HashMap<>();
    fields.put(
        "host", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.KEYWORD).build());
    fields.put(
        "message", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.TEXT).build());
    fields.put("ip", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.IP).build());
    fields.put(
        "myTimestamp",
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.DATE).build());
    fields.put(
        "success", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.BOOLEAN).build());
    fields.put(
        "cost", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.DOUBLE).build());
    fields.put(
        "amount", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.FLOAT).build());
    fields.put(
        "amount_half_float",
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.HALF_FLOAT).build());
    fields.put(
        "value", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.INTEGER).build());
    fields.put(
        "count", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.LONG).build());
    fields.put(
        "count_scaled_long",
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.SCALED_LONG).build());
    fields.put(
        "count_short",
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.SHORT).build());
    fields.put(
        "bucket", Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.BINARY).build());

    schema = Schema.IngestSchema.newBuilder().putAllFields(fields).build();
  }

  @Test
  public void testSimpleSchema() {
    Trace.KeyValue kv = SpanFormatter.convertKVtoProto("host", "host1", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getVStr()).isEqualTo("host1");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);

    kv = SpanFormatter.convertKVtoProto("message", "my message", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getVStr()).isEqualTo("my message");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.TEXT);

    kv = SpanFormatter.convertKVtoProto("ip", "8.8.8.8", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getVStr()).isEqualTo("8.8.8.8");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.IP);

    kv = SpanFormatter.convertKVtoProto("myTimestamp", "2021-01-01T00:00:00Z", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getVStr()).isEqualTo("2021-01-01T00:00:00Z");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.DATE);

    kv = SpanFormatter.convertKVtoProto("success", "true", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.BOOL);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.BOOLEAN);
    assertThat(kv.getVBool()).isEqualTo(true);

    kv = SpanFormatter.convertKVtoProto("success", true, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.BOOL);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.BOOLEAN);
    assertThat(kv.getVBool()).isEqualTo(true);

    kv = SpanFormatter.convertKVtoProto("cost", "10.0", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.FLOAT64);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.DOUBLE);
    assertThat(kv.getVFloat64()).isEqualTo(10.0);

    kv = SpanFormatter.convertKVtoProto("cost", 10.0, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.FLOAT64);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.DOUBLE);
    assertThat(kv.getVFloat64()).isEqualTo(10.0);

    kv = SpanFormatter.convertKVtoProto("amount", "10.0", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.FLOAT32);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.FLOAT);
    assertThat(kv.getVFloat32()).isEqualTo(10.0f);

    kv = SpanFormatter.convertKVtoProto("amount", 10.0, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.FLOAT32);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.FLOAT);
    assertThat(kv.getVFloat32()).isEqualTo(10.0f);

    kv = SpanFormatter.convertKVtoProto("amount_half_float", "10.0", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.FLOAT32);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.HALF_FLOAT);
    assertThat(kv.getVFloat32()).isEqualTo(10.0f);

    kv = SpanFormatter.convertKVtoProto("amount_half_float", 10.0, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.FLOAT32);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.HALF_FLOAT);
    assertThat(kv.getVFloat32()).isEqualTo(10.0f);

    kv = SpanFormatter.convertKVtoProto("value", "10", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.INT32);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.INTEGER);
    assertThat(kv.getVInt32()).isEqualTo(10);

    kv = SpanFormatter.convertKVtoProto("value", 10, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.INT32);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.INTEGER);
    assertThat(kv.getVInt32()).isEqualTo(10);

    kv = SpanFormatter.convertKVtoProto("count", "10", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.INT64);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.LONG);
    assertThat(kv.getVInt64()).isEqualTo(10L);

    kv = SpanFormatter.convertKVtoProto("count", 10, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.INT64);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.LONG);
    assertThat(kv.getVInt64()).isEqualTo(10L);

    kv = SpanFormatter.convertKVtoProto("count_scaled_long", "10", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.INT64);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.SCALED_LONG);
    assertThat(kv.getVInt64()).isEqualTo(10);

    kv = SpanFormatter.convertKVtoProto("count_scaled_long", 10, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.INT64);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.SCALED_LONG);
    assertThat(kv.getVInt64()).isEqualTo(10L);

    kv = SpanFormatter.convertKVtoProto("count_short", "10", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.INT32);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.SHORT);
    assertThat(kv.getVInt32()).isEqualTo(10L);

    kv = SpanFormatter.convertKVtoProto("count_short", 10, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.INT32);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.SHORT);
    assertThat(kv.getVInt32()).isEqualTo(10);

    kv = SpanFormatter.convertKVtoProto("bucket", "e30=", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.BINARY);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.BINARY);
    assertThat(kv.getVBinary().toStringUtf8()).isEqualTo("e30=");
  }

  @Test
  public void testKeyValueWithWrongValues() {
    Trace.KeyValue kv = SpanFormatter.convertKVtoProto("success", "notBoolean", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.BOOL);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.BOOLEAN);
    assertThat(kv.getVBool()).isEqualTo(false);

    kv = SpanFormatter.convertKVtoProto("cost", "hello", schema);
    assertThat(kv.getKey()).isEqualTo("failed_cost");
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("hello");

    kv = SpanFormatter.convertKVtoProto("amount", "hello", schema);
    assertThat(kv.getKey()).isEqualTo("failed_amount");
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("hello");

    kv = SpanFormatter.convertKVtoProto("amount_half_float", "half_float_value", schema);
    assertThat(kv.getKey()).isEqualTo("failed_amount_half_float");
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("half_float_value");

    kv = SpanFormatter.convertKVtoProto("value", "int_value", schema);
    assertThat(kv.getKey()).isEqualTo("failed_value");
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("int_value");

    kv = SpanFormatter.convertKVtoProto("count", "long_value", schema);
    assertThat(kv.getKey()).isEqualTo("failed_count");
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("long_value");

    kv = SpanFormatter.convertKVtoProto("count_scaled_long", "scaled_long_val", schema);
    assertThat(kv.getKey()).isEqualTo("failed_count_scaled_long");
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("scaled_long_val");

    kv = SpanFormatter.convertKVtoProto("count_short", "my_short-Val", schema);
    assertThat(kv.getKey()).isEqualTo("failed_count_short");
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("my_short-Val");
  }

  @Test
  public void testSimpleWithoutSchema() {

    Schema.IngestSchema schema = Schema.IngestSchema.getDefaultInstance();
    Trace.KeyValue kv = SpanFormatter.convertKVtoProto("host", "host1", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getVStr()).isEqualTo("host1");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);

    kv = SpanFormatter.convertKVtoProto("ip", "8.8.8.8", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getVStr()).isEqualTo("8.8.8.8");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);

    kv = SpanFormatter.convertKVtoProto("myTimestamp", "2021-01-01T00:00:00Z", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getVStr()).isEqualTo("2021-01-01T00:00:00Z");
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);

    kv = SpanFormatter.convertKVtoProto("success", "true", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("true");

    kv = SpanFormatter.convertKVtoProto("success", true, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.BOOL);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.BOOLEAN);
    assertThat(kv.getVBool()).isEqualTo(true);

    kv = SpanFormatter.convertKVtoProto("cost", "10.0", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("10.0");

    kv = SpanFormatter.convertKVtoProto("amount", 10.0f, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.FLOAT32);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.FLOAT);
    assertThat(kv.getVFloat32()).isEqualTo(10.0f);

    kv = SpanFormatter.convertKVtoProto("cost", 10.0, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.FLOAT64);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.DOUBLE);
    assertThat(kv.getVFloat64()).isEqualTo(10.0);

    kv = SpanFormatter.convertKVtoProto("value", 10, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.INT32);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.INTEGER);

    kv = SpanFormatter.convertKVtoProto("count", 10L, schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.INT64);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.LONG);
    assertThat(kv.getVInt64()).isEqualTo(10L);

    kv = SpanFormatter.convertKVtoProto("bucket", "e30=", schema);
    assertThat(kv.getVType()).isEqualTo(Trace.ValueType.STRING);
    assertThat(kv.getFieldType()).isEqualTo(Schema.SchemaFieldType.KEYWORD);
    assertThat(kv.getVStr()).isEqualTo("e30=");
  }
}
