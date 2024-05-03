package com.slack.astra.logstore.schema;

import com.slack.astra.logstore.LogMessage;
import com.slack.astra.proto.schema.Schema;

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
}
