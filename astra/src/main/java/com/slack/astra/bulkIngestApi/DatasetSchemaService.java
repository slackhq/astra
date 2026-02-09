package com.slack.astra.bulkIngestApi;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.astra.logstore.schema.ReservedFields;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.schema.SchemaMetadata;
import com.slack.astra.metadata.schema.SchemaMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that maintains schema configuration consistent with the value stored in the
 * SchemaMetadataStore. Watches for SchemaMetadata changes and updates the schema accordingly.
 *
 * <p>There is one schema per cluster, stored under SchemaMetadata.GLOBAL_SCHEMA_NAME.
 */
public class DatasetSchemaService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetSchemaService.class);

  private final SchemaMetadataStore schemaMetadataStore;
  private final AstraMetadataStoreChangeListener<SchemaMetadata> schemaListener =
      (_) -> updateSchema();

  private volatile Schema.IngestSchema schema;
  private volatile AstraConfigs.SchemaMode schemaMode;

  public DatasetSchemaService(SchemaMetadataStore schemaMetadataStore) {
    this.schemaMetadataStore = schemaMetadataStore;
    this.schema = ReservedFields.addPredefinedFields(Schema.IngestSchema.getDefaultInstance());
    this.schemaMode = AstraConfigs.SchemaMode.SCHEMA_MODE_DYNAMIC;
  }

  private void updateSchema() {
    try {
      List<SchemaMetadata> schemas = schemaMetadataStore.listSync();

      for (SchemaMetadata schemaMeta : schemas) {
        if (schemaMeta.getSchema() != null && schemaMeta.getSchema().getFieldsCount() > 0) {
          Schema.IngestSchema newSchema =
              ReservedFields.addPredefinedFields(schemaMeta.getSchema());
          this.schema = newSchema;
          this.schemaMode = schemaMeta.getSchemaMode();
          LOG.info(
              "Updated schema from SchemaMetadataStore '{}': {} fields, mode={}",
              schemaMeta.getName(),
              newSchema.getFieldsCount(),
              schemaMode);
          return;
        }
      }

      // No schema found - use defaults
      this.schema = ReservedFields.addPredefinedFields(Schema.IngestSchema.getDefaultInstance());
      this.schemaMode = AstraConfigs.SchemaMode.SCHEMA_MODE_DYNAMIC;
      LOG.info("No schema found in SchemaMetadataStore, using default schema");
    } catch (Exception e) {
      LOG.error("Error updating schema from SchemaMetadataStore", e);
    }
  }

  @Override
  protected void startUp() throws Exception {
    updateSchema();
    schemaMetadataStore.addListener(schemaListener);
  }

  @Override
  protected void shutDown() throws Exception {
    schemaMetadataStore.removeListener(schemaListener);
  }

  /** Returns the current schema. Thread-safe via volatile read. */
  public Schema.IngestSchema getSchema() {
    return schema;
  }

  /** Returns the current schema mode. Thread-safe via volatile read. */
  public AstraConfigs.SchemaMode getSchemaMode() {
    return schemaMode;
  }
}
