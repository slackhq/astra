package com.slack.astra.logstore.schema;

import com.slack.astra.proto.schema.Schema;

public class ReservedFields {

  public static final String TIMESTAMP = "@timestamp";

  public static Schema.IngestSchema addPredefinedFields(Schema.IngestSchema currentSchema) {
    Schema.SchemaField timestampField =
        Schema.SchemaField.newBuilder().setType(Schema.SchemaFieldType.DATE).build();
    return Schema.IngestSchema.newBuilder()
        .putAllFields(currentSchema.getFieldsMap())
        .putFields(TIMESTAMP, timestampField)
        .build();
  }
}
