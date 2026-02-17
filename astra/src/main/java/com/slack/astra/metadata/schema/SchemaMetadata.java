package com.slack.astra.metadata.schema;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.astra.metadata.core.AstraMetadata;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
import java.util.Objects;

/**
 * Metadata for the cluster-wide ingestion schema. There is one schema per cluster (each cluster has
 * its own ZK/etcd namespace), stored under a well-known constant name.
 */
public class SchemaMetadata extends AstraMetadata {

  public static final String GLOBAL_SCHEMA_NAME = "global_schema";

  public final Schema.IngestSchema schema;
  public final AstraConfigs.SchemaMode schemaMode;

  public SchemaMetadata(
      String name, Schema.IngestSchema schema, AstraConfigs.SchemaMode schemaMode) {
    super(name);
    checkArgument(schema != null, "schema must not be null");
    checkArgument(schemaMode != null, "schemaMode must not be null");
    this.schema = schema;
    this.schemaMode = schemaMode;
  }

  public Schema.IngestSchema getSchema() {
    return schema;
  }

  public AstraConfigs.SchemaMode getSchemaMode() {
    return schemaMode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SchemaMetadata)) return false;
    if (!super.equals(o)) return false;
    SchemaMetadata that = (SchemaMetadata) o;
    return schema.equals(that.schema) && schemaMode == that.schemaMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), schema, schemaMode);
  }

  @Override
  public String toString() {
    return "SchemaMetadata{"
        + "name='"
        + name
        + '\''
        + ", fieldsCount="
        + schema.getFieldsCount()
        + ", defaultsCount="
        + schema.getDefaultsCount()
        + ", schemaMode="
        + schemaMode
        + '}';
  }
}
