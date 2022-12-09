package com.slack.kaldb.logstore.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.slack.kaldb.logstore.DocumentBuilder;
import com.slack.kaldb.logstore.FieldDefMismatchException;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import com.slack.kaldb.util.JsonUtil;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SchemaAwareLogDocumentBuilder always indexes a field using the same type. It doesn't allow field
 * conflicts.
 *
 * <p>In case of a field conflict, this class uses FieldConflictPolicy to handle them.
 *
 * <p>NOTE: Currently, if building a document raises errors, we still register the type of the
 * fields in this document partially. While the document may not be indexed, this partial field
 * config will exist in the system. For now, we assume storing this metadata is fine since it is
 * rarely an issue and helps with performance. If this is an issue, we need to scan the json twice
 * to ensure document is good to index.
 */
public class SchemaAwareLogDocumentBuilderImpl implements DocumentBuilder<LogMessage> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SchemaAwareLogDocumentBuilderImpl.class);

  // TODO: In future, make this value configurable.
  private static final int MAX_NESTING_DEPTH = 3;

  private static void addTextField(
      ImmutableMap.Builder<String, LuceneFieldDef> fieldDefBuilder,
      String fieldName,
      boolean isStored,
      boolean isIndexed) {
    fieldDefBuilder.put(
        fieldName, new LuceneFieldDef(fieldName, FieldType.TEXT.name, isStored, isIndexed, false));
  }

  // TODO: Move this definition to the config file.
  private static ImmutableMap<String, LuceneFieldDef> getDefaultLuceneFieldDefinitions() {
    ImmutableMap.Builder<String, LuceneFieldDef> fieldDefBuilder = ImmutableMap.builder();
    addTextField(fieldDefBuilder, LogMessage.SystemField.SOURCE.fieldName, true, false);
    addTextField(fieldDefBuilder, LogMessage.SystemField.ALL.fieldName, false, true);
    fieldDefBuilder.put(
        LogMessage.SystemField.ID.fieldName,
        new LuceneFieldDef(
            LogMessage.SystemField.ID.fieldName, FieldType.STRING.name, false, true, true));
    fieldDefBuilder.put(
        LogMessage.SystemField.INDEX.fieldName,
        new LuceneFieldDef(
            LogMessage.SystemField.INDEX.fieldName, FieldType.TEXT.name, false, true, false));

    fieldDefBuilder.put(
        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
        new LuceneFieldDef(
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            FieldType.LONG.name,
            false,
            true,
            true));
    addTextField(fieldDefBuilder, LogMessage.ReservedField.TIMESTAMP.fieldName, false, true);
    fieldDefBuilder.put(
        LogMessage.SystemField.TYPE.fieldName,
        new LuceneFieldDef(
            LogMessage.SystemField.TYPE.fieldName, FieldType.STRING.name, false, true, true));

    addTextField(fieldDefBuilder, LogMessage.ReservedField.HOSTNAME.fieldName, false, true);
    addTextField(fieldDefBuilder, LogMessage.ReservedField.PACKAGE.fieldName, false, true);
    addTextField(fieldDefBuilder, LogMessage.ReservedField.MESSAGE.fieldName, false, true);
    addTextField(fieldDefBuilder, LogMessage.ReservedField.TAG.fieldName, false, true);
    addTextField(fieldDefBuilder, LogMessage.ReservedField.USERNAME.fieldName, false, true);
    addTextField(fieldDefBuilder, LogMessage.ReservedField.PAYLOAD.fieldName, false, true);
    addTextField(fieldDefBuilder, LogMessage.ReservedField.NAME.fieldName, false, true);
    fieldDefBuilder.put(
        LogMessage.ReservedField.SERVICE_NAME.fieldName,
        new LuceneFieldDef(
            LogMessage.ReservedField.SERVICE_NAME.fieldName,
            FieldType.STRING.name,
            false,
            true,
            true));
    fieldDefBuilder.put(
        LogMessage.ReservedField.DURATION_MS.fieldName,
        new LuceneFieldDef(
            LogMessage.ReservedField.DURATION_MS.fieldName,
            FieldType.LONG.name,
            false,
            true,
            true));
    fieldDefBuilder.put(
        LogMessage.ReservedField.TRACE_ID.fieldName,
        new LuceneFieldDef(
            LogMessage.ReservedField.TRACE_ID.fieldName, FieldType.STRING.name, false, true, true));
    fieldDefBuilder.put(
        LogMessage.ReservedField.PARENT_ID.fieldName,
        new LuceneFieldDef(
            LogMessage.ReservedField.PARENT_ID.fieldName,
            FieldType.STRING.name,
            false,
            true,
            true));

    return fieldDefBuilder.build();
  }

  /**
   * This enum tracks the field conflict policy for a chunk.
   *
   * <p>NOTE: In future, we may need this granularity at a per field level. Also, other potential
   * options for handling these conflicts: (a) store all the conflicted fields as strings by default
   * so querying those fields is more consistent. (b) duplicate field value only but don't create a
   * field.
   */
  public enum FieldConflictPolicy {
    // Throw an error on field conflict.
    RAISE_ERROR,
    // Drop the conflicting field.
    DROP_FIELD,
    // Convert the field value to the type of the conflicting field.
    CONVERT_FIELD_VALUE,
    // Convert the field value to the type of conflicting field and also create a new field of type.
    CONVERT_AND_DUPLICATE_FIELD
  }

  private static final String PLACEHOLDER_FIELD_NAME = "PLACEHOLDER_FIELD_NAME";
  private static final Map<FieldType, LuceneFieldDef> defaultPropDescriptionForType =
      ImmutableMap.of(
          FieldType.LONG,
          new LuceneFieldDef(PLACEHOLDER_FIELD_NAME, FieldType.LONG.name, false, true, true),
          FieldType.FLOAT,
          new LuceneFieldDef(PLACEHOLDER_FIELD_NAME, FieldType.FLOAT.name, false, true, true),
          FieldType.INTEGER,
          new LuceneFieldDef(PLACEHOLDER_FIELD_NAME, FieldType.INTEGER.name, false, true, true),
          FieldType.DOUBLE,
          new LuceneFieldDef(PLACEHOLDER_FIELD_NAME, FieldType.DOUBLE.name, false, true, true),
          FieldType.TEXT,
          new LuceneFieldDef(PLACEHOLDER_FIELD_NAME, FieldType.TEXT.name, false, true, false),
          FieldType.STRING,
          new LuceneFieldDef(PLACEHOLDER_FIELD_NAME, FieldType.STRING.name, false, true, true),
          FieldType.BOOLEAN,
          new LuceneFieldDef(PLACEHOLDER_FIELD_NAME, FieldType.BOOLEAN.name, false, true, false));

  @VisibleForTesting
  public FieldConflictPolicy getIndexFieldConflictPolicy() {
    return indexFieldConflictPolicy;
  }

  private void addField(
      final Document doc,
      final String key,
      final Object value,
      final String keyPrefix,
      int nestingDepth) {
    // If value is a list, convert the value to a String and index the field.
    if (value instanceof List) {
      addField(doc, key, Strings.join((List) value, ','), keyPrefix, nestingDepth);
      return;
    }

    String fieldName = keyPrefix.isBlank() || keyPrefix.isEmpty() ? key : keyPrefix + "." + key;
    // Ingest nested map field recursively upto max nesting. After that index it as a string.
    if (value instanceof Map) {
      if (nestingDepth >= MAX_NESTING_DEPTH) {
        // Once max nesting depth is reached, index the field as a string.
        addField(doc, key, value.toString(), keyPrefix, nestingDepth + 1);
      } else {
        Map<Object, Object> mapValue = (Map<Object, Object>) value;
        for (Object k : mapValue.keySet()) {
          if (k instanceof String) {
            addField(doc, (String) k, mapValue.get(k), fieldName, nestingDepth + 1);
          } else {
            throw new FieldDefMismatchException(
                String.format(
                    "Field %s, %s has an non-string type which is unsupported", k, value));
          }
        }
      }
      return;
    }

    FieldType valueType = getJsonType(value);
    if (!fieldDefMap.containsKey(fieldName)) {
      indexNewField(doc, fieldName, value, valueType);
    } else {
      LuceneFieldDef registeredField = fieldDefMap.get(fieldName);
      // If the field types are same or the fields are type aliases
      if (registeredField.fieldType == valueType
          || FieldType.areTypeAliasedFieldTypes(registeredField.fieldType, valueType)) {
        // No field conflicts index it using previous description.
        // Pass in registeredField here since the valueType and registeredField may be aliases
        indexTypedField(doc, fieldName, value, registeredField);
      } else {
        // There is a field type conflict, index it using the field conflict policy.
        switch (indexFieldConflictPolicy) {
          case DROP_FIELD:
            LOG.debug("Dropped field {} due to field type conflict", fieldName);
            droppedFieldsCounter.increment();
            break;
          case CONVERT_FIELD_VALUE:
            convertValueAndIndexField(value, valueType, registeredField, doc, fieldName);
            LOG.debug(
                "Converting field {} value from type {} to {} due to type conflict",
                fieldName,
                valueType,
                registeredField.fieldType);
            convertFieldValueCounter.increment();
            break;
          case CONVERT_AND_DUPLICATE_FIELD:
            convertValueAndIndexField(value, valueType, registeredField, doc, fieldName);
            LOG.debug(
                "Converting field {} value from type {} to {} due to type conflict",
                fieldName,
                valueType,
                registeredField.fieldType);
            // Add new field with new type
            String newFieldName = makeNewFieldOfType(fieldName, valueType);
            indexNewField(doc, newFieldName, value, valueType);
            LOG.debug(
                "Added new field {} of type {} due to type conflict", newFieldName, valueType);
            convertAndDuplicateFieldCounter.increment();
            break;
          case RAISE_ERROR:
            throw new FieldDefMismatchException(
                String.format(
                    "Field type for field %s is %s but new value is of type  %s. ",
                    fieldName, registeredField.fieldType, valueType));
        }
      }
    }
  }

  private void indexNewField(Document doc, String key, Object value, FieldType valueType) {
    // If we are seeing a field for the first time index it with default template for the
    // valueType and create a field def.
    if (!defaultPropDescriptionForType.containsKey(valueType)) {
      throw new RuntimeException("No default prop description");
    }

    LuceneFieldDef defaultPropDescription = defaultPropDescriptionForType.get(valueType);
    // add the document to this field.
    fieldDefMap.put(key, defaultPropDescription);
    indexTypedField(doc, key, value, defaultPropDescription);
  }

  static String makeNewFieldOfType(String key, FieldType valueType) {
    return key + "_" + valueType.getName();
  }

  private static void convertValueAndIndexField(
      Object value, FieldType valueType, LuceneFieldDef registeredField, Document doc, String key) {
    Object convertedValue =
        FieldType.convertFieldValue(value, valueType, registeredField.fieldType);
    if (convertedValue == null) {
      throw new RuntimeException("No mapping found to convert value");
    }
    indexTypedField(doc, key, convertedValue, registeredField);
  }

  private static void indexTypedField(
      Document doc, String key, Object value, LuceneFieldDef fieldDef) {
    fieldDef.fieldType.addField(doc, key, value, fieldDef);
  }

  private static FieldType getJsonType(Object value) {
    if (value instanceof Long) {
      return FieldType.LONG;
    }
    if (value instanceof Integer) {
      return FieldType.INTEGER;
    }
    if (value instanceof String) {
      return FieldType.TEXT;
    }
    if (value instanceof Float) {
      return FieldType.FLOAT;
    }
    if (value instanceof Boolean) {
      return FieldType.BOOLEAN;
    }
    if (value instanceof Double) {
      return FieldType.DOUBLE;
    }

    throw new RuntimeException("Unknown type");
  }

  public static SchemaAwareLogDocumentBuilderImpl build(
      FieldConflictPolicy fieldConflictPolicy,
      boolean enableFullTextSearch,
      MeterRegistry meterRegistry) {
    // Add basic fields by default
    return new SchemaAwareLogDocumentBuilderImpl(
        fieldConflictPolicy,
        getDefaultLuceneFieldDefinitions(),
        enableFullTextSearch,
        meterRegistry);
  }

  static final String DROP_FIELDS_COUNTER = "dropped_fields";
  static final String CONVERT_FIELD_VALUE_COUNTER = "convert_field_value";
  static final String CONVERT_AND_DUPLICATE_FIELD_COUNTER = "convert_and_duplicate_field";

  private final FieldConflictPolicy indexFieldConflictPolicy;
  private final Map<String, LuceneFieldDef> fieldDefMap = new ConcurrentHashMap<>();
  private final boolean enableFullTextSearch;
  private final Counter droppedFieldsCounter;
  private final Counter convertFieldValueCounter;
  private final Counter convertAndDuplicateFieldCounter;

  SchemaAwareLogDocumentBuilderImpl(
      FieldConflictPolicy indexFieldConflictPolicy,
      final Map<String, LuceneFieldDef> initialFields,
      boolean enableFullTextSearch,
      MeterRegistry meterRegistry) {
    this.indexFieldConflictPolicy = indexFieldConflictPolicy;
    fieldDefMap.putAll(initialFields);
    this.enableFullTextSearch = enableFullTextSearch;
    // Note: Consider adding field name as a tag to help debugging, but it's high cardinality.
    droppedFieldsCounter = meterRegistry.counter(DROP_FIELDS_COUNTER);
    convertFieldValueCounter = meterRegistry.counter(CONVERT_FIELD_VALUE_COUNTER);
    convertAndDuplicateFieldCounter = meterRegistry.counter(CONVERT_AND_DUPLICATE_FIELD_COUNTER);
  }

  @Override
  public Document fromMessage(LogMessage message) throws JsonProcessingException {
    Document doc = new Document();
    addField(doc, LogMessage.SystemField.INDEX.fieldName, message.getIndex(), "", 0);
    addField(
        doc, LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, message.timeSinceEpochMilli, "", 0);
    addField(doc, LogMessage.SystemField.TYPE.fieldName, message.getType(), "", 0);
    addField(doc, LogMessage.SystemField.ID.fieldName, message.id, "", 0);

    final String msgString = JsonUtil.writeAsString(message.toWireMessage());
    addField(doc, LogMessage.SystemField.SOURCE.fieldName, msgString, "", 0);
    if (enableFullTextSearch) {
      addField(doc, LogMessage.SystemField.ALL.fieldName, msgString, "", 0);
    }

    for (String key : message.source.keySet()) {
      addField(doc, key, message.source.get(key), "", 0);
    }
    return doc;
  }

  @Override
  public Map<String, LuceneFieldDef> getSchema() {
    return fieldDefMap;
  }
}
