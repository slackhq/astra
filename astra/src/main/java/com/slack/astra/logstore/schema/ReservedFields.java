package com.slack.astra.logstore.schema;

import com.slack.astra.logstore.LogMessage;
import com.slack.astra.proto.schema.Schema;
import java.util.Map;

public class ReservedFields {

  public static final String TIMESTAMP = "@timestamp";

  public static Schema.IngestSchema addPredefinedFields(Schema.IngestSchema currentSchema) {
    Schema.SchemaField timestampField =
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.DATE).build();
    Schema.SchemaField messageField =
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.TEXT).build();
    Schema.SchemaField allField =
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.TEXT).build();
    Schema.SchemaField timeSinceEpoch =
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.LONG).build();
    Schema.SchemaField idField =
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.ID).build();

    return Schema.IngestSchema.newBuilder()
        .setEnableKeywordSubfield(currentSchema.getEnableKeywordSubfield())
        .setIgnoreAboveSubfield(currentSchema.getIgnoreAboveSubfield())
        .putAllFields(currentSchema.getFieldsMap())
        .putFields(TIMESTAMP, timestampField)
        .putFields(LogMessage.ReservedField.MESSAGE.fieldName, messageField)
        .putFields(LogMessage.SystemField.ALL.fieldName, allField)
        .putFields(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, timeSinceEpoch)
        .putFields(LogMessage.SystemField.ID.fieldName, idField)
        .build();
  }

  public static Schema.IngestSchema START_SCHEMA =
      addPredefinedFields(Schema.IngestSchema.getDefaultInstance());

  public static Schema.SchemaField getSchemaFieldForType(Schema.SchemaFieldType type) {
    return predefinedFields.getOrDefault(
        type, Schema.SchemaField.newBuilder().setType(type).build());
  }

  private static final Map<Schema.SchemaFieldType, Schema.SchemaField> predefinedFields =
      getPredefinedFields();

  private static Map<Schema.SchemaFieldType, Schema.SchemaField> getPredefinedFields() {
    return Map.of(
        Schema.SchemaFieldType.BOOLEAN,
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.BOOLEAN).build(),
        Schema.SchemaFieldType.INTEGER,
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.INTEGER).build(),
        Schema.SchemaFieldType.LONG,
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.LONG).build(),
        Schema.SchemaFieldType.FLOAT,
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.FLOAT).build(),
        Schema.SchemaFieldType.DOUBLE,
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.DOUBLE).build(),
        Schema.SchemaFieldType.KEYWORD,
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.KEYWORD).build(),
        Schema.SchemaFieldType.TEXT,
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.TEXT).build());
  }
}
